fn main() {
    prost_build::compile_protos(
        &[
            "src/proto/pop/addressmetadata.proto",
            "src/proto/pop/paymentrequest.proto",
            "src/proto/relay/messages.proto",
            "src/proto/relay/filters.proto",
        ],
        &["src/"],
    )
    .unwrap();
}
