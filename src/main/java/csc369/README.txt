1.
I used RequestCountByCountry as my initial job, which contained a reduce-side join, and outputs
(country, count), although it needs another step to be properly aggregated since the pairs come from
different mappers. The reduce-side join was in favor of the thought that neither of the inputs are
known to be small, and the inputs can presumably be extremely large. There is another job called
AggregateByCountry, as they are otherwise separated because they come from different data sources,
and are coded as such by the mappers. The third job, AccessLog2, comes from switching key,value
pairs and using a descending comparator.
First input/output: input_access_log hostname_countries output1,
Second input/output: output1, output1_1,
Third input/output: output1_1, output1_2

2.
I started off with CountURLs, which was a reduce-side join on hostname again, from which
(country, URL) pairs were made, and then aggregated in AggregateByCountryURL, which is a collection
or records of unique (country, URL) pairs, which are then sorted by SortByCountryThenCount, since
AccessLog2 only sorts based off of value, but this new sort job uses country as a key and then
sorts (url, count) pairs using a comparator within the reduce class. The inputs and outputs are below,
and the jobs are in the order just listed.
File chain:
input_access_log hostname_countries ->
output2 ->
output2_1 ->
output2_2

3.
The first job here is CountURLs3 (sorry for the naming, I'm not too creative), where instead of emitting
a ((country, url), 1), emit url as the key and the country as the value. Then, in AggregateByURL,
there is some black magic (TreeSet) and of course, sets don't allow for duplicates. Then, the country
names that remain in the set are joined together by commas, and voila.

files:
input_access_log hostname_countries ->
output3 ->
output3_1
