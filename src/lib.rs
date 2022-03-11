#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);

        let ns = i64::MAX / (1000_000_000) / 60 / 60 / 24;
        panic!("{}", ns);
    }
}
