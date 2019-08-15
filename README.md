# SaavnTop100TrendingSongs

Write and execute a MapReduce program to figure out the top 100 trending songs from Saavn’s stream data, on a daily basis, for the week December 25-31. Although this is a real-time streaming problem, you may use all the data till the (n-1)th
 day to calculate your output for the nth day, i.e. you may consider all the stream data till 24 December (included) in your program to find the trending songs for 25 December and so on.
 
 The term ‘trending songs’ may be defined loosely as those songs that have gathered relatively high numbers of streams within small time windows (e.g. the last four hours) and have also shown positive increases in their stream growth rates.
 
 A stream is a record of a user playing a song. Each stream is represented as a tuple with the following attributes:

(song ID, user ID, timestamp, hour, date)

 

Each tuple consists of the song ID of the streamed song, the user ID of the user who streamed the song, the timestamp (Unix) of the stream, the hour of streaming, and the date of streaming.
