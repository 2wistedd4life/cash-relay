use std::sync::Arc;

use cashweb::relay::*;
use prost::Message as PMessage;
use rocksdb::{Direction, Error as RocksError, IteratorMode, Options, DB};

use crate::models::wrapper::AuthWrapper;

const DIGEST_LEN: usize = 4;
const NAMESPACE_LEN: usize = 20 + 1;

const DIGEST_NAMESPACE: u8 = b'd';
pub const FEED_NAMESPACE: u8 = b'f';
pub const MESSAGE_NAMESPACE: u8 = b'm';
const PROFILE_NAMESPACE: u8 = b'p';

#[derive(Clone)]
pub struct Database(Arc<DB>);

pub fn msg_key(pubkey_hash: &[u8], timestamp: u64, digest: &[u8], namespace: u8) -> Vec<u8> {
    let raw_timestamp: [u8; 8] = timestamp.to_be_bytes();
    [
        pubkey_hash,
        &[namespace],
        &raw_timestamp,
        &digest[..DIGEST_LEN],
    ]
    .concat()
}

pub fn msg_prefix(pubkey_hash: &[u8], timestamp: u64, namespace: u8) -> Vec<u8> {
    let raw_timestamp: [u8; 8] = timestamp.to_be_bytes();
    [&pubkey_hash[..], &[namespace], &raw_timestamp].concat()
}

impl Database {
    pub fn try_new(path: &str) -> Result<Self, RocksError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        DB::open(&opts, &path).map(Arc::new).map(Database)
    }

    pub fn get_msg_key_by_digest(
        &self,
        pubkey_hash: &[u8],
        digest: &[u8],
        namespace: u8,
    ) -> Result<Option<Vec<u8>>, RocksError> {
        let digest_key = [pubkey_hash, &[DIGEST_NAMESPACE], &digest].concat();

        let opt_timestamp = self.0.get(digest_key)?;
        Ok(opt_timestamp.map(|timestamp| {
            [pubkey_hash, &[namespace], &timestamp, &digest[..DIGEST_LEN]].concat()
        }))
    }

    pub fn remove_message_by_digest(
        &self,
        pubkey_hash: &[u8],
        digest: &[u8],
        namespace: u8,
    ) -> Result<Option<()>, RocksError> {
        match self.get_msg_key_by_digest(pubkey_hash, digest, namespace)? {
            Some(some) => {
                self.0.delete(&some)?;
                Ok(Some(()))
            }
            None => Ok(None),
        }
    }

    pub fn push_message(
        &self,
        pubkey_hash: &[u8],
        timestamp: u64,
        raw_message: &[u8],
        digest: &[u8],
        namespace: u8,
    ) -> Result<(), RocksError> {
        // Create key
        let raw_timestamp: [u8; 8] = timestamp.to_be_bytes();
        let key = [
            &pubkey_hash[..],
            &[namespace],
            &raw_timestamp,
            &digest[..DIGEST_LEN],
        ]
        .concat();
        self.0.put(key, raw_message)?;

        // Create digest key
        let digest_key = [&pubkey_hash[..], &[DIGEST_NAMESPACE], &digest].concat();

        self.0.put(digest_key, raw_timestamp)?;

        Ok(())
    }

    pub fn get_message_by_digest(
        &self,
        pubkey_hash: &[u8],
        digest: &[u8],
        namespace: u8,
    ) -> Result<Option<Vec<u8>>, RocksError> {
        match self.get_msg_key_by_digest(pubkey_hash, digest, namespace)? {
            Some(some) => self.get_message_by_key(&some),
            None => Ok(None),
        }
    }

    pub fn get_message_by_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.0.get(key)
    }

    pub fn get_messages_range(
        &self,
        start_prefix: &[u8],
        opt_end_prefix: Option<&[u8]>,
    ) -> Result<MessagePage, RocksError> {
        let namespace = &start_prefix[..NAMESPACE_LEN]; // addr || msg namespace byte

        // Check whether key is within namespace
        let in_namespace = |key: &[u8]| key[..NAMESPACE_LEN] == namespace[..];

        // Init iterator
        let iter = self
            .0
            .iterator(IteratorMode::From(&start_prefix, Direction::Forward));

        let messages: Vec<Message> = if let Some(end_prefix) = opt_end_prefix {
            // Check whether key is before end time
            let before_end_key = |key: &[u8]| key[NAMESPACE_LEN..] < end_prefix[NAMESPACE_LEN..];

            // Take items inside namespace and before end time
            iter.take_while(|(key, _)| in_namespace(key) && before_end_key(key))
                .map(|(_, item)| {
                    Message::decode(&item[..]).unwrap() // This panics if stored bytes are malformed
                })
                .collect()
        } else {
            // Take items inside namespace
            iter.take_while(|(key, _)| in_namespace(key))
                .map(|(_, item)| {
                    Message::decode(&item[..]).unwrap() // This panics if stored bytes are malformed
                })
                .collect()
        };

        let mut message_page = MessagePage::default();
        if let Some(message) = messages.first() {
            message_page.start_time = message.received_time;
            let payload_digest = message.digest().unwrap(); // This is safe
            message_page.start_digest = payload_digest.to_vec();
        }
        if let Some(message) = messages.last() {
            message_page.start_time = message.received_time;
            let payload_digest = message.digest().unwrap(); // This is safe
            message_page.start_digest = payload_digest.to_vec();
        }
        message_page.messages = messages;
        Ok(message_page)
    }

    pub fn remove_messages_range(
        &self,
        start_prefix: &[u8],
        opt_end_prefix: Option<&[u8]>,
    ) -> Result<(), RocksError> {
        let namespace = &start_prefix[..NAMESPACE_LEN]; // addr || msg namespace byte

        // Check whether key is within namespace
        let in_namespace = |key: &[u8]| key[..NAMESPACE_LEN] == namespace[..];

        // Init iterator
        let iter = self
            .0
            .iterator(IteratorMode::From(&start_prefix, Direction::Forward));

        if let Some(end_prefix) = opt_end_prefix {
            // Check whether key is before end time
            let before_end_key = |key: &[u8]| key[NAMESPACE_LEN..] < end_prefix[NAMESPACE_LEN..];

            // Take items inside namespace and before end time
            let iter = iter.take_while(|(key, _)| in_namespace(key) && before_end_key(key));

            for (key, _) in iter {
                self.0.delete(key)?;
            }
        } else {
            // Take items inside namespace
            let iter = iter.take_while(|(key, _)| in_namespace(key));

            for (key, _) in iter {
                self.0.delete(key)?;
            }
        };

        Ok(())
    }

    pub fn get_raw_profile(&self, addr: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        // Prefix key
        let key = [addr, &[PROFILE_NAMESPACE]].concat();

        self.0.get(key)
    }

    pub fn get_profile(&self, addr: &[u8]) -> Result<Option<AuthWrapper>, RocksError> {
        self.get_raw_profile(addr).map(|raw_profile_opt| {
            raw_profile_opt.map(|raw_profile| {
                AuthWrapper::decode(&raw_profile[..]).unwrap() // This panics if stored bytes are malformed
            })
        })
    }

    pub fn put_profile(&self, addr: &[u8], raw_profile: &[u8]) -> Result<(), RocksError> {
        // Prefix key
        let key = [addr, &[PROFILE_NAMESPACE]].concat();

        self.0.put(key, raw_profile)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoincash_addr::Address;
    use ring::digest::{digest, SHA256};

    #[test]
    fn get_digest() {
        let database = Database::try_new("./test_dbs/get_digest").unwrap();

        let addr = Address::decode("bchtest:qz35wy0grm4tze4p5tvu0fc6kujsa5vnrcr7y5xl65").unwrap();
        let address_payload = addr.as_body();

        let message = Message::default();
        let mut raw_message = Vec::with_capacity(message.encoded_len());
        message.encode(&mut raw_message).unwrap();
        let digest = digest(&SHA256, &raw_message);

        let timestamp = 100;
        database
            .push_message(
                &address_payload,
                timestamp,
                &raw_message[..],
                digest.as_ref(),
                MESSAGE_NAMESPACE,
            )
            .unwrap();

        assert!(database
            .get_msg_key_by_digest(&address_payload, digest.as_ref(), MESSAGE_NAMESPACE)
            .unwrap()
            .is_some());
    }

    #[test]
    fn delete_digest() {
        let database = Database::try_new("./test_dbs/delete_digest").unwrap();

        let addr = Address::decode("bchtest:qz35wy0grm4tze4p5tvu0fc6kujsa5vnrcr7y5xl65").unwrap();
        let address_payload = addr.as_body();

        let message = Message::default();
        let mut raw_message = Vec::with_capacity(message.encoded_len());
        message.encode(&mut raw_message).unwrap();
        let digest = digest(&SHA256, &raw_message);

        let timestamp = 100;
        database
            .push_message(
                &address_payload,
                timestamp,
                &raw_message[..],
                digest.as_ref(),
                MESSAGE_NAMESPACE,
            )
            .unwrap();

        assert!(database
            .get_msg_key_by_digest(&address_payload, digest.as_ref(), MESSAGE_NAMESPACE)
            .unwrap()
            .is_some());

        assert!(database
            .remove_message_by_digest(&address_payload, digest.as_ref(), MESSAGE_NAMESPACE)
            .unwrap()
            .is_some());

        assert!(database
            .get_message_by_digest(&address_payload, digest.as_ref(), MESSAGE_NAMESPACE)
            .unwrap()
            .is_none())
    }

    #[test]
    fn get_time_range() {
        let database = Database::try_new("./test_dbs/get_time_range").unwrap();

        let addr = Address::decode("bchtest:qz35wy0grm4tze4p5tvu0fc6kujsa5vnrcr7y5xl65").unwrap();
        let address_payload = addr.as_body();

        let mut message = Message::default();
        message.payload_digest = vec![0; 32];
        let mut raw_message = Vec::with_capacity(message.encoded_len());
        message.encode(&mut raw_message).unwrap();
        let digest = digest(&SHA256, &raw_message);

        // Put at 100 and 105
        database
            .push_message(
                &address_payload,
                100,
                &raw_message[..],
                digest.as_ref(),
                MESSAGE_NAMESPACE,
            )
            .unwrap();
        database
            .push_message(
                &address_payload,
                105,
                &raw_message[..],
                digest.as_ref(),
                MESSAGE_NAMESPACE,
            )
            .unwrap();

        // Check out of range [106, inf)
        let prefix = msg_prefix(&address_payload, 106, MESSAGE_NAMESPACE);
        assert_eq!(
            database.get_messages_range(&prefix, None).unwrap().messages,
            vec![]
        );

        // Check within range [100, inf)
        let prefix = msg_prefix(&address_payload, 100, MESSAGE_NAMESPACE);
        assert_eq!(
            database
                .get_messages_range(&prefix, None)
                .unwrap()
                .messages
                .len(),
            2
        );

        // Check within range [100, 101)
        let prefix = msg_prefix(&address_payload, 100, MESSAGE_NAMESPACE);
        let prefix_end = msg_prefix(&address_payload, 101, MESSAGE_NAMESPACE);
        assert_eq!(
            database
                .get_messages_range(&prefix, Some(&prefix_end))
                .unwrap()
                .messages
                .len(),
            1
        );

        // Check within range [101, 105)
        let prefix = msg_prefix(&address_payload, 101, MESSAGE_NAMESPACE);
        let prefix_end = msg_prefix(&address_payload, 105, MESSAGE_NAMESPACE);
        assert_eq!(
            database
                .get_messages_range(&prefix, Some(&prefix_end))
                .unwrap()
                .messages
                .len(),
            0
        )
    }
}
