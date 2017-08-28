# stat-xplore-R
R project used to scrape data from DWP's Stat-Xplore API

Stat-Xplore is the UK Department of Work and Pensions' portal to view their published statistics on benefits such as Universal Credit, Carers Allowance (CA) and disability benefits like Personal Independence Payments (PIP) and Disability Living Allowance (DLA). You can find it at https://stat-xplore.dwp.gov.uk. In order to to use the API, you need to sign up for a free account to get an access key for the API. Accessing the browser part of the portal is also useful as a way of determining what the query for the API should be.

This project consists of three main files:
schema.R allows you to build a local version of the Stat-Xplore database map. You don't strictly need to do this, but it's useful both for identifying what the query should be and for automating certain things. I recommend that you do use this.
scraping.R does the bulk of the work.
scraping_templates.R gives a few examples of how to actually get something from the API.

# Getting Started
Download the project and open it
Install the packages
Source schema.R and scraping.R
See .Renviron section below
Run build_schema() (takes a couple of minutes)
Run df = get_scotland_CA_entitled() to see what the results look like
Experiment/explore to work out how to get the data that you want

# .Renviron
I keep some sensitive and person specific information in my .Renviron file (this is specific to RStudio. You may need to just edit the information into the code if you aren't using RStudio). This includes my Stat-Xplore API key and proxy settings. To edit the file, run file.edit('~/.Renviron') and just type in the info according to the following template, filling the quotes with the relevant info:
stat_xplore_key = ""
proxy_address = ""
proxy_port = ""
proxy_username = ""
proxy_password = ""

If you aren't on a proxy, or you are but you don't need a username and password for it, I don't know whether you can just leave these blank, or if you need to remove the sections of code. If the latter, it's around line 135 in scraping.R and line 83 in schema.R that you need to look.

Once you've edited the .Renviron file, close RStudio and then open the project up again to implement the changes.

# Notes
I don't work for DWP, so I can't help if any of the data on Stat-Xplore is wrong, inconsistent, out of date etc. Feel free to get in touch if the code doesn't work as expected, though.

Stat-Xplore uses Space Time Research's SuperWEB2 (browser based system) and Open Data API. Much of this project ought to be transferrable to other databases in this system, with not too much additional effort. http://docs.spacetimeresearch.com/display/SuperSTAR99/Introduction
