--ANSWER1
CREATE TABLE query1 AS
SELECT g.name::text AS name, COUNT(c.genreid)::bigint AS moviecount
FROM genres g
INNER JOIN hasagenre c ON g.genreid = c.genreid
GROUP BY g.genreid;


--ANSWER2
CREATE TABLE query2 AS
SELECT g.name::text AS name, AVG(r.rating)::numeric AS rating
FROM genres g
INNER JOIN hasagenre c ON g.genreid = c.genreid
INNER JOIN ratings r ON c.movieid = r.movieid
GROUP BY g.name;


--ANSWER3
CREATE TABLE query3 AS
SELECT m.title::text AS title, COUNT(ratings.userid)::bigint AS countofratings
FROM ratings
INNER JOIN movies m ON ratings.movieid = m.movieid
GROUP BY m.title
HAVING COUNT(ratings.userid) >= 10;


--ANSWER4
CREATE TABLE query4 AS
SELECT m.movieid::integer AS movieid, m.title::text AS title
FROM movies m
INNER JOIN hasagenre h ON m.movieid = h.movieid
INNER JOIN genres g ON h.genreid = g.genreid
WHERE g.name = 'Comedy';


--ANSWER5
CREATE TABLE query5 AS
SELECT m.title::text AS title, AVG(r.rating)::numeric AS average
FROM ratings r
INNER JOIN movies m ON r.movieid = m.movieid
GROUP BY m.title;


--ANSWER6
CREATE TABLE query6 AS
SELECT AVG(r.rating)::numeric AS average
FROM movies m
INNER JOIN ratings r ON m.movieid = r.movieid
INNER JOIN hasagenre h ON m.movieid = h.movieid
INNER JOIN genres g ON h.genreid = g.genreid
WHERE g.name = 'Comedy';


--ANSWER7
CREATE TABLE query7 AS 
SELECT AVG(r.rating)::numeric AS average
FROM ratings r
INNER JOIN hasagenre h ON r.movieid = h.movieid
INNER JOIN genres g ON h.genreid = g.genreid
WHERE g.name = 'Comedy' OR g.name = 'Romance';


--ANSWER8
CREATE TABLE query8 AS 
SELECT AVG(r.rating)::numeric AS average
FROM ratings r
INNER JOIN hasagenre h ON r.movieid = h.movieid
INNER JOIN genres g ON h.genreid = g.genreid
WHERE g.name = 'Romance'
AND r.movieid NOT IN (
    SELECT h2.movieid
    FROM hasagenre h2
    INNER JOIN genres g2 ON h2.genreid = g2.genreid
    WHERE g2.name = 'Comedy'
);


--ANSWER9
CREATE TABLE query9 AS
SELECT r.movieid::integer AS movieid, r.rating::numeric AS rating
FROM ratings r
INNER JOIN users u ON r.userid = u.userid
WHERE u.userid = :v1;
