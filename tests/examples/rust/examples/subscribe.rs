use std::process;
use tdengine::Subscriber;

fn main() {
    let subscriber = Subscriber::new("127.0.0.1", "root", "taosdata", "demo", "m1", 0, 1000)
                        .unwrap_or_else(|err| {
        eprintln!("Can't create Subscriber: {}", err);
        process::exit(1)
    });

    loop {
        let row = subscriber.consume().unwrap_or_else(|err| {
            eprintln!("consume exit: {}", err);
            process::exit(1)
        });

        subscriber.print_row(&row);
    }
}
