Critical Stations Analysis
==========================

Analysis of the data received from sensors located in various stations, in order to evaluate critical situations. 
A critical situation, is a situation where the number of free slots is below a user defined threshold.
The exercise is composed of three parts:

- **Part 1**: store the stations with a percentage of critical situations equal or higher than 80%.
- **Part 2**: compute the percentage of critical situations for each pair (timeslot, station). Timeslot
can assume one of the six values: [0-3], [4-7], .., [20-23].
- **Part 3**: select the lines from the readings file where the following constraints are true:
    - The line is associated with a full station situation
    - All the neighbor stations of the previous station are full in the same timestamp (date:hour:minute).
    
**Input**:
- **readings**: stationID,date,hour,minute,num_of_bikes,num_free_slots.
- **neighbors**: stationID,stationIDx stationIDy