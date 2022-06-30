use std::{io::stdout, time::Duration};

use futures::{future::FutureExt, select, StreamExt};

use tokio::time::interval;

use crossterm::{
    cursor::position,
    event::{DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode},
    Result,
};

const HELP: &str = r#"EventStream based on futures_util::Stream with tokio
 - Keyboard, mouse and terminal resize events enabled
 - Prints "." every second if there's no event
 - Hit "c" to print current cursor position
 - Use Esc to quit
"#;

async fn print_events() {
    let mut reader = EventStream::new();

    loop {
        let mut interval = interval(Duration::from_millis(1_000));
        let mut event = reader.next();
        match event.await {
            Some(Ok(event)) => {
                println!("Event::{:?}\r", event);

                if event == Event::Key(KeyCode::Char('c').into()) {
                    println!("Cursor position: {:?}\r", position());
                }

                if event == Event::Key(KeyCode::Esc.into()) {
                    break;
                }
            }
            Some(Err(e)) => println!("Error: {:?}\r", e),
            None => break,
        }

        // select! {
        //     // _ = interval.tick() => { println!(".\r"); },
        //     maybe_event = event => {
        //         match maybe_event {
        //             Some(Ok(event)) => {
        //                 println!("Event::{:?}\r", event);

        //                 if event == Event::Key(KeyCode::Char('c').into()) {
        //                     println!("Cursor position: {:?}\r", position());
        //                 }

        //                 if event == Event::Key(KeyCode::Esc.into()) {
        //                     break;
        //                 }
        //             }
        //             Some(Err(e)) => println!("Error: {:?}\r", e),
        //             None => break,
        //         }
        //     }
        // };
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("{}", HELP);

    enable_raw_mode()?;

    let mut stdout = stdout();

    print_events().await

    disable_raw_mode()
}
