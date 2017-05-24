Movies Example
==============

file 1 movies defined by <id, name>
file 2 movies defined by <id, duration, type_ids>
MapReduce1 generates list of movies and duration
MapReduce2 divides movies based on duration into two different outputs with local cache for
type of movie

Why?
- Natural Join pattern
- Multiple MapReduce programs
- Local cache with couple <id, movie type>