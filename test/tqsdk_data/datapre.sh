rm rb*.*
python3 bardl.py -n rb1910 -g 60
python2 integrate_minute_k_line.py -s rb1910_60.json -i 13
