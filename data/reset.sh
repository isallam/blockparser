objy deletefd -boot bitcoin.boot
objy createfd -fdname bitcoin
objy do -infile ../config/allschema.txt -boot bitcoin.boot 
# objy addIndex -name blockIndex -class Block -attribute m_Id -bootfile bitcoin.boot 
# objy addIndex -name addressIndex -class Address -attribute m_Hash -bootfile bitcoin.boot 
# objy addIndex -name transactionIndex -class Transaction -attribute m_Hash -bootfile bitcoin.boot 
# objy addIndex -name tagIndex -class Tag -attribute m_Label -bootfile bitcoin.boot 
objy importPlacement -infile ../config/bitcoin.pmd -boot bitcoin.boot
