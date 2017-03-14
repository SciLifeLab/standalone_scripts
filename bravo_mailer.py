
import smtplib
import argparse

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def main(args):
	# Allow HTML-formatted emails (very simplistic atm, should be expanded if used)
	msg = MIMEMultipart("alternative")
	if args["body"].startswith("<html>", 0, 10):
		msg.attach(MIMEText(args["body"],"html"))
	else:
		msg.attach(MIMEText(args["body"],"plain"))

	msg["Subject"] = args["sub"]
	msg["From"] = args["from"]
	msg["To"] = args["to"]

	s = smtplib.SMTP(args["smtp"])

	# If authentication is required:
	# s.starttls()
	# s.login(user, pass)

	s.sendmail(args["from"], [args["to"]], msg.as_string())
	s.quit()

if __name__ == "__main__":
	p = argparse.ArgumentParser(description="Send an email")
	p.add_argument("--to", "-t", required=True, help="To address")
	p.add_argument("--from", "-f", required=True, help="From address")
	p.add_argument("--sub", "-s", required=True, help="Subject")
	p.add_argument("--body", "-b", required=True, help="Message body")
	p.add_argument("--smtp", default="localhost", help="SMTP server")
	args = p.parse_args()
	main(vars(args))
