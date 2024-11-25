import os
import logging
import requests
from bs4 import BeautifulSoup
import argparse
from scraper_core import BillScraper



def main(args):
    scraper = BillScraper(args.session)

    # set up directories if necessary
    os.makedirs(args.output_dir, exist_ok=True)
    
    # start logging
    logging.basicConfig(
        handlers=[
            logging.FileHandler(args.output_dir+"bills_scraper.log"), # write the logs to a file
            logging.StreamHandler() # also write logs to the console
            ],
        format="%(asctime)s:%(levelname)s:%(message)s"
    )

    # Set the log level for the file handler to INFO
    file_handler = logging.getLogger().handlers[0]
    file_handler.setLevel(logging.INFO)

    # Set the log level for the stream handler based on args.loglevel
    stream_handler = logging.getLogger().handlers[1]
    stream_handler.setLevel(args.loglevel.upper())

    logging.info("######### NEW RUN #########")

    # download and parse list of PDF links from the URL
    res = requests.get(scraper.ld_base_url)
    soup = BeautifulSoup(res.content, features="html.parser")
    hrefs = [a.attrs["href"] for a in soup.find_all("a")[1:]] # get all links except to parent directory
    lds = [href.split('/')[-1][:-4] for href in hrefs] # turn the list of links into a list of lds

    # process the links
    bill_count = 0
    testimony_count = 0
    for ld in lds:

        # skip bills that are already in the corpus
        if os.path.isfile(args.output_dir + ld + ".txt"):
            logging.debug("{} already in corpus.".format(ld))
        else:
            # otherwise, start processing the bill    
            logging.info("Processing {}".format(ld))
            
            try:
                # try to download the pdf
                logging.debug("Downloading PDF for {}".format(ld))
                pdf = scraper.download_bill(ld, args.output_dir)
            except requests.exceptions.RequestException as e:
                # skip if the download fails. This happens frequently
                logging.warning("Download error for {}; will retry on next run".format(ld))
                continue
            except FileNotFoundError as e:
                # Log a separate error if the issue is related to writing the PDF
                logging.warning("Could not write pdf for {}; will retry on next run".format(ld))
                continue

            # perform the conversion
            logging.debug("Converting LD {}".format(ld))
            scraper.pdf_to_txt(pdf)

            # clean up the PDF document
            logging.debug("Removing PDF for {}".format(ld))
            os.remove(pdf)

            bill_count += 1

        if args.testimony:
            # don't download testimony for amendments (they don't have separate testimony)
            if len(ld.split('-')) > 3:
                continue
            # download the testimony
            logging.debug("Checking testimony for {}".format(ld))
            ldno = ld.split('-')[2]
            testimony = scraper.get_testimony(ldno)
            if testimony:
                t_dir = os.path.join(args.testimony, ld)
                os.makedirs(t_dir, exist_ok=True)
                for t in testimony:
                    document_name = f'{t["FirstName"]} {t["LastName"]} ({t["Organization"]}) - LD {ldno}.pdf'
                    
                    if os.path.isfile(os.path.join(t_dir, document_name.replace('.pdf', '.txt'))):
                        logging.debug("Testimony for LD {}/{} already in corpus.".format(ldno, document_name))
                        continue
                    pdf = scraper.download_testimony(t["Id"], document_name, t_dir)
                    logging.debug("Converting testimony for LD {}/{}".format(ldno, document_name))
                    scraper.pdf_to_txt(pdf)
                    logging.debug("Removing PDF for LD {}/{}".format(ldno, document_name))
                    os.remove(pdf)
                    testimony_count += 1
    
    # log success message
    logging.info(f"Added {bill_count} new bills and {testimony_count} pieces of testimony to corpus.")

if __name__ == "__main__":
    # create command line arguments to choose the legislative session and set the output directory
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--session", default="131", type=str, help="number of the legislative session to run")
    parser.add_argument("-o", "--output_dir", default="./131/bills/", type=str, help="path to store bills in")
    parser.add_argument("-t", "--testimony", type=str, nargs="?", default=None, const="./131/testimony/", help="path to store testimony in")
    parser.add_argument( '-log', '--loglevel', default='warning', help='Provide logging level. Example --loglevel debug' )
    args = parser.parse_args()

    # run the main function
    main(args)
    
    
