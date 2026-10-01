"""
Fields which we can confidently ignore in each model.

It should be safe to import this entire set into all new models and thereby ignore all of these fields without
further checks.

Therefore, **it is crucial for any new fields added to this set to be added to the decision log.**
"""

IGNORE_SYSTEM_KEYS = {
    "0247_9",  # provenance of the DOI
    "0248_a",
    "0248_p",
    "0248_q",
    "852__c",  # holdings will be taken separately
    "852__h",
    "030__a",  # From SIS: all 030__* fields can be ignored
    "035__h",  # From SIS: all 035__* fields can be ignored
    "035__d",  # From SIS: all 035__* fields can be ignored
    "035__m",  # From SIS: all 035__* fields can be ignored
    "035__t",  # From SIS: all 035__* fields can be ignored
    "035__u",  # From SIS: all 035__* fields can be ignored
    "035__z",  # From SIS: all 035__* fields can be ignored
    "037__c",  # arxiv subject
    "100__m",  # email of contributor
    "245__9",  # Provenance of title
    "260__c",  # From SIS: see decision log
    "269__c",  # From SIS: see decision log
    "300__a",  # number of pages
    "340__a",  # See decision log
    "500__9",  # Provenance of the note
    "520__9",  # Provenance of the description
    "536__r",  # From SIS: see decision log
    "540__3",  # Material of the license
    "540__9",  # Also material of the license
    "540__g",  # From SIS (see decision log)
    "542__3",  # Also material of the license
    "595__i",  # From SIS: see decision log
    "700__m",  # email of contributor
    "773__t",  # from SIS: can be ignored
    "773__0",  # from SIS: can be ignored
    "773__o",  # from SIS: can be ignored
    "773__x",  # INSPIRE publication note
    "8564_8",  # file id
    "8564_s",  # bibdoc id
    "8564_x",  # icon thumbnails sizes
    "8564_y",  # file description - done by files dump
    "8564_8",  # File information (done by file dump)
    "8564_q",  # File links File information (done by file dump)
    "8564_z",  # Websubmit "stamp" (migrated as file metadata)
    "905__m",  # Submitter email address
    "916__y",  # year, redundant value
    "937__c",  # last modified by
    "937__s",  # last modification date
    "960__a",  # base number
    "961__c",  # CDS modification tag # TODO
    "961__h",  # CDS modification tag # TODO
    "961__l",  # CDS modification tag # TODO
    "961__x",  # CDS modification tag # TODO
    "981__a",  # duplicate record id
    "999C50",  # From SIS: all 999* fields can be ignored
    "999C52",  # https://cds.cern.ch/record/2640188/export/hm?ln=en
    "999C59",  # https://cds.cern.ch/record/2284615/export/hm?ln=en
    "999C5a",  # https://cds.cern.ch/record/2678429/export/hm?ln=en
    "999C5c",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5h",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5i",  # https://cds.cern.ch/record/2284892/export/hm?ln=en
    "999C5k",  # https://cds.cern.ch/record/2671914/export/hm?ln=en
    "999C5l",  # https://cds.cern.ch/record/2283115/export/hm?ln=en
    "999C5m",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5o",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5p",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5r",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5s",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5u",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5v",  # https://cds.cern.ch/record/2283088/export/hm?ln=en
    "999C5x",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
    "999C5y",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5z",  # https://cds.cern.ch/record/2710809/export/hm?ln=en
    "999C6a",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C6t",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C6v",  # https://cds.cern.ch/record/2284606/export/hm?ln=en
    "999C5d",  # old INSPIRE attr
    "999C69",  # See decision log
    "999C6c",  # See decision log
}
