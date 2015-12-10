#!/usr/bin/python

import sys

current_game = None
current_sum = 0
game = None

# input comes from STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()

	# parse the input we got from mapper.py
	game, profit = line.split('\t', 1)

	# convert profit (currently a string) to float
	try:
		profit = float(profit)
	except ValueError:
		# profit was not a number, so silently
		# ignore/discard this line
		continue

	# this IF-switch only works because Hadoop sorts map output
	# by key (here: game) before it is passed to the reducer
	if current_game == game:
		current_sum += profit
	else:
		if current_game:
			# write result to STDOUT
			print '%s\t%s' % (current_game, current_sum)
		current_sum = profit
		current_game = game

# do not forget to output the last game if needed!
if current_game == game:
	print '%s\t%s' % (current_game, current_sum)