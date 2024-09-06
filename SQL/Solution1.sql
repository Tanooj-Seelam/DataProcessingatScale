CREATE TABLE IF NOT EXISTS users (
    userid INT,
    name TEXT NOT NULL,
    PRIMARY KEY (userid)
);

CREATE TABLE IF NOT EXISTS movies (
    movieid INT,
    title TEXT NOT NULL,
    PRIMARY KEY (movieid)
);

CREATE TABLE IF NOT EXISTS taginfo (
    tagid INT PRIMARY KEY,
    content TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS genres (
    genreid INT PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS ratings (
    userid INT NOT NULL,
    movieid INT NOT NULL,
    rating NUMERIC(2,1) CHECK (rating >= 0 AND rating <= 5) NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (userid, movieid),
    FOREIGN KEY (userid) REFERENCES users(userid) ON DELETE CASCADE,
    FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tags (
    userid INT NOT NULL,
    movieid INT NOT NULL,
    tagid INT NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (userid, movieid, tagid),
    FOREIGN KEY (userid) REFERENCES users(userid) ON DELETE CASCADE,
    FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE,
    FOREIGN KEY (tagid) REFERENCES taginfo(tagid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hasagenre (
    movieid INT NOT NULL,
    genreid INT NOT NULL,
    PRIMARY KEY (movieid, genreid),
    FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE,
    FOREIGN KEY (genreid) REFERENCES genres(genreid) ON DELETE CASCADE
);
