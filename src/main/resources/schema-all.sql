DROP TABLE book IF EXISTS;

CREATE TABLE book(
    id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    title VARCHAR(90),
    author VARCHAR(40),
    isbn VARCHAR(13),
    publisher VARCHAR(20),
    year NUMBER
);