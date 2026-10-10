use rbus::provider::Prop;

#[derive(rbus::Row)]
struct WithProp {
    enable: Prop<bool>,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
#[rbus(path = "Device.A.")]
struct WithPath {
    enable: bool,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
struct WritableObject {
    #[rbus(object, writable)]
    stats: Stats,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
struct Stats {
    bytes: u64,
}

fn main() {}
