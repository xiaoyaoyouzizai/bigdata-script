#!/usr/bin/python

import sys

for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()
	# split the line into words
	words = line.split(",")
	# times lotteryid ordertypeid amount
	try:
		amount = float(words[13][1:-1])
	except ValueError:
		# profit was not a number, so silently
		# ignore/discard this line
		continue

	ordertypeid = words[11][1:-1]
	# print '%s\t%s\t%s\t%s' % (words[23][1:14],words[1][1:-1],ordertypeid,amount)
	if amount == 0:
		continue
	if ordertypeid in ["3", "6", "7", "11", "12"]:
		print '%s,%s\t%s' % (words[23][1:14], words[1][1:-1], str(amount))
	elif ordertypeid in ["9", "4", "5"]:
		print '%s,%s\t%s' % (words[23][1:14], words[1][1:-1], "-" + str(amount))
	else:
		continue

	# print '%s\t%s\t%s\t%s' % (words[23][1:14], words[1][1:-1], str(float(amount)), ordertypeid)
	
