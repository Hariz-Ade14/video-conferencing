use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::mpsc::{self, error::TrySendError};

pub type ConnId = u64;

/// How many messages may queue for one socket before we treat it as stuck.
/// Signaling is bursty (a dozen ICE candidates at once) but small.
pub const OUTBOX_CAPACITY: usize = 256;

struct Member {
    /// The browser's own id (`from` in its messages), learned from the first
    /// message it sends. Used for `to` routing and the goodbye on disconnect.
    peer_id: Option<String>,
    tx: mpsc::Sender<Arc<str>>,
}

/// Who is connected to which room. Each socket owns the receiving half of an
/// mpsc channel; relaying a message means pushing it onto the other members'
/// channels. Nothing is stored once a room empties.
#[derive(Clone, Default)]
pub struct Rooms {
    rooms: Arc<Mutex<HashMap<String, HashMap<ConnId, Member>>>>,
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RoomFull;

impl Rooms {
    pub fn is_full(&self, room: &str, max: usize) -> bool {
        self.rooms.lock().unwrap().get(room).is_some_and(|m| m.len() >= max)
    }

    pub fn join(&self, room: &str, tx: mpsc::Sender<Arc<str>>, max: usize) -> Result<ConnId, RoomFull> {
        let mut rooms = self.rooms.lock().unwrap();
        let members = rooms.entry(room.to_string()).or_default();
        if members.len() >= max {
            return Err(RoomFull);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        members.insert(id, Member { peer_id: None, tx });
        Ok(id)
    }

    /// Ties a socket to the peer id it speaks for. The first id a socket uses
    /// sticks; afterwards it may not send as anyone else, and two sockets in
    /// one room may not share an id. Returns false if the message should be
    /// dropped.
    pub fn claim_peer_id(&self, room: &str, conn: ConnId, peer_id: &str) -> bool {
        let mut rooms = self.rooms.lock().unwrap();
        let Some(members) = rooms.get_mut(room) else { return false };
        let taken_by_other = members
            .iter()
            .any(|(id, m)| *id != conn && m.peer_id.as_deref() == Some(peer_id));
        let Some(member) = members.get_mut(&conn) else { return false };
        match &member.peer_id {
            Some(existing) => existing == peer_id,
            None if taken_by_other => false,
            None => {
                member.peer_id = Some(peer_id.to_string());
                true
            }
        }
    }

    /// Sends `payload` to the other members of `room`, or only to the member
    /// whose peer id is `to`. A socket whose queue is full is disconnected,
    /// and the rest of the room is told it left.
    pub fn relay(&self, room: &str, from: ConnId, to: Option<&str>, payload: Arc<str>) {
        let mut stuck = Vec::new();
        {
            let rooms = self.rooms.lock().unwrap();
            let Some(members) = rooms.get(room) else { return };
            for (id, member) in members {
                if *id == from || to.is_some_and(|to| member.peer_id.as_deref() != Some(to)) {
                    continue;
                }
                if let Err(TrySendError::Full(_)) = member.tx.try_send(payload.clone()) {
                    stuck.push(*id);
                }
            }
        }
        for id in stuck {
            tracing::warn!(room, conn = id, "outbox full, disconnecting");
            self.leave(room, id);
        }
    }

    /// Removes a socket. If it had announced itself, the others in the room
    /// get a `leave` on its behalf, so a crashed tab or dropped network is
    /// noticed straight away instead of when WebRTC times out.
    pub fn leave(&self, room: &str, conn: ConnId) {
        let peer_id = {
            let mut rooms = self.rooms.lock().unwrap();
            let Some(members) = rooms.get_mut(room) else { return };
            // Dropping the member drops its sender, which ends that socket's loop.
            let Some(member) = members.remove(&conn) else { return };
            if members.is_empty() {
                rooms.remove(room);
            }
            member.peer_id
        };
        if let Some(peer_id) = peer_id {
            let goodbye = serde_json::json!({ "room": room, "message": { "type": "leave", "from": peer_id } });
            self.relay(room, conn, None, Arc::from(goodbye.to_string()));
        }
    }

    /// (rooms, connections), for the health endpoint.
    pub fn stats(&self) -> (usize, usize) {
        let rooms = self.rooms.lock().unwrap();
        (rooms.len(), rooms.values().map(HashMap::len).sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(rooms: &Rooms, room: &str) -> (ConnId, mpsc::Receiver<Arc<str>>) {
        let (tx, rx) = mpsc::channel(OUTBOX_CAPACITY);
        (rooms.join(room, tx, 10).unwrap(), rx)
    }

    #[test]
    fn peer_ids_cannot_be_changed_or_shared() {
        let rooms = Rooms::default();
        let (a, _ra) = member(&rooms, "r");
        let (b, _rb) = member(&rooms, "r");
        assert!(rooms.claim_peer_id("r", a, "alice"));
        assert!(rooms.claim_peer_id("r", a, "alice"));
        assert!(!rooms.claim_peer_id("r", a, "mallory"), "a socket can't switch identity");
        assert!(!rooms.claim_peer_id("r", b, "alice"), "b can't impersonate a");
    }

    #[test]
    fn room_is_removed_when_last_member_leaves() {
        let rooms = Rooms::default();
        let (a, _ra) = member(&rooms, "r");
        assert_eq!(rooms.stats(), (1, 1));
        rooms.leave("r", a);
        assert_eq!(rooms.stats(), (0, 0));
    }

    #[test]
    fn join_respects_room_size() {
        let rooms = Rooms::default();
        let (tx, _rx) = mpsc::channel(1);
        rooms.join("r", tx.clone(), 1).unwrap();
        assert_eq!(rooms.join("r", tx, 1), Err(RoomFull));
    }

    #[test]
    fn stuck_member_is_dropped_and_others_told() {
        let rooms = Rooms::default();
        let (a, mut ra) = member(&rooms, "r");
        let (tx, _never_read) = mpsc::channel(1);
        let slow = rooms.join("r", tx, 10).unwrap();
        assert!(rooms.claim_peer_id("r", slow, "slow"));

        // First message fills the slow member's queue; the second overflows it.
        rooms.relay("r", a, None, Arc::from("1"));
        rooms.relay("r", a, None, Arc::from("2"));

        assert_eq!(rooms.stats(), (1, 1), "slow member was removed");
        let goodbye = ra.try_recv().unwrap();
        assert!(goodbye.contains(r#""type":"leave""#) && goodbye.contains(r#""from":"slow""#));
    }
}
