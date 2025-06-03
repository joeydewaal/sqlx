use bytes::BytesMut;

pub trait Codec {
    type Request;

    fn encode(&mut self, buf: &mut Vec<u8>, request: Self::Request) -> crate::Result<()>;

    type Response;

    fn decode(&mut self, buf: &mut BytesMut) -> crate::Result<Option<Self::Response>>;
}
