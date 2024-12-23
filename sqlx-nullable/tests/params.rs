use nullable::{NullableState, Source, SqlFlavour};

#[test]
pub fn params_1() {
    let mut source = Source::empty();
    source.add_params(vec![false]);

    let query = r#"
        select $1
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn params_2() {
    let mut source = Source::empty();
    source.add_params(vec![true]);

    let query = r#"
        select $1
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn params_3() {
    let mut source = Source::empty();
    source.add_params(vec![false, true]);

    let query = r#"
        select $1, $1
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?", "?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn params_4() {
    let mut source = Source::empty();
    source.add_params(vec![false, true]);

    let query = r#"
        select $2, $2
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?", "?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true])
}

#[test]
pub fn params_5() {
    let mut source = Source::empty();
    source.add_params(vec![false]);

    let query = r#"
        select ?
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Sqlite);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn params_6() {
    let mut source = Source::empty();
    source.add_params(vec![true]);

    let query = r#"
        select ?
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Sqlite);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn params_7() {
    let mut source = Source::empty();
    source.add_params(vec![false, true]);

    let query = r#"
        select ?, ?
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Sqlite);
    let nullable = state.get_nullable(&["?column?", "?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true])
}
