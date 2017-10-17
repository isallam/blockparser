objy deletefd -boot bitcoin.boot
objy createfd -fdname bitcoin
objy do -infile ../config/allschema.txt -boot bitcoin.boot 
objy importPlacement -infile ../config/bitcoin.pmd -boot bitcoin.boot
