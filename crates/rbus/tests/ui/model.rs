use rbus::provider::{Event, Prop, Table};

#[derive(rbus::Row, Clone, PartialEq, Default)]
struct Host {
    name: String,
}

#[derive(rbus::Model)]
#[rbus(path = "Device.A")]
struct NoDot {
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
#[rbus(path = "Device.A.{i}.")]
struct Templated {
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
struct Typo {
    #[rbus(writeable)]
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
struct WritableTable {
    #[rbus(writable)]
    hosts: Table<Host>,
}

#[derive(rbus::Model)]
struct ReadOnlyValidate {
    #[rbus(validate = check)]
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
struct EventWithRows {
    #[rbus(add_row)]
    alarm: Event<()>,
}

#[derive(rbus::Model)]
struct Dotted {
    #[rbus(name = "A.B")]
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
struct Twice {
    #[rbus(name = "Enable")]
    on: Prop<bool>,
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
#[rbus(on_commit = Hooks::commit)]
struct NoPath {
    enable: Prop<bool>,
}

#[derive(rbus::Model)]
struct Tuple(Prop<bool>);

fn check(_: &bool) -> Result<(), rbus::provider::Fault> {
    Ok(())
}

fn main() {}
