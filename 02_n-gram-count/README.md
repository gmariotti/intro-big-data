N-Gram Count
============

Count n-words at a time, n from terminal, then the combiner filters the list with sequences that
start with a user input as a config property and the reducer counts them and do filters again.
Combiner are not always called. Use a counter for each sequence removed