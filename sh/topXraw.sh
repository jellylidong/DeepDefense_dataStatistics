# src, dst, data_length, protocol_name, protocol_number
tshark -r ../pcap/*.pcap -T fields -e ip.src -e ip.dst -e frame.len -e col.Protocol  -e ip.proto -e frame.time_epoch -E separator=, >> ../csv/topXraw.csv

