import sys
import re
import json
import fileinput
import argparse

def print_json(files):
	samples = {}
	for line in fileinput.input(files=files):
		m = re.search(r"P\d{4}_\d{3}", line)
		if m:
			s = m.group()
		else:
			l = line.split("=")
			if len(l) > 2:
				d = [x.strip(" -") for x in l]
				samples.setdefault(s, {})[d[1]] = d[0]

	print(json.dumps(samples))

def print_csv(files):
	samples = {}
	for line in fileinput.input(files=files):
		m = re.search(r"P\d{4}_\d{3}", line)
		if m:
			samples.setdefault("SAMPLE", []).append(m.group())
		else:
			l = line.split("=")
			if len(l) > 2:
				d = [x.strip(" -") for x in l]
				samples.setdefault(d[1], []).append(d[0])

	data = []
	for k,v in samples.items():
		data.append([k] + v)

	data = list(map(list, zip(*data))) # Transpose
	for row in data:
		print(",".join(row))

def main(args):
	if args.csv:
		print_csv(args.files)
	else:
		print_json(args.files)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Parse Supernova reports and print numbers in JSON or CSV")
	parser.add_argument('--csv', action="store_true", default=False,
		help='Output in CSV format, default is JSON')
	parser.add_argument('files', metavar='FILE', nargs='*',
		help='Files to parse, if empty or "-" stdin is used')
	args = parser.parse_args()
	main(args)