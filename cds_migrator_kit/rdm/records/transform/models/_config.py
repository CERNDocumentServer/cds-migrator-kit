"""Fields which we can confidently ignore in each model."""

IGNORE_SYSTEM_KEYS = {
    "0247_9",  # provenance of the DOI
    "0248_a",
    "0248_p",
    "0248_q",
    "852__c",  # holdings will be taken separately
    "852__h",
    "035__h",  # OAI harvest tag or timestamp
    "035__d",  # OAI harvest tag or timestamp
    "035__m",  # OAI harvest format (e.g. `marcxml`)
    "035__t",  # oai harvest tag
    "035__u",  # oai harvest tag
    "035__z",  # oai harvest tag
    "037__c",  # arxiv subject
    "100__m",  # email of contributor
    "245__9",  # Provenance of title
    "270__m",  # Contact email
    "300__a",  # number of pages
    "520__9",  # Provenance of the description
    "540__3",  # Material of the license
    "540__9",  # Also material of the license
    "542__3",  # Also material of the license
    "700__m",  # email of contributor
    "773__t",  # from SIS: can be ignored
    "773__0",  # from SIS: can be ignored
    "773__o",  # from SIS: can be ignored
    "773__x",  # INSPIRE publication note
    "8564_8",  # file id
    "8564_s",  # bibdoc id
    "8564_x",  # icon thumbnails sizes
    "8564_y",  # file description - done by files dump
    "8564_z",  # Websubmit "stamp" (migrated as file metadata)
    "916__y",  # year, redundant value
    "937__c",  # last modified by
    "937__s",  # last modification date
    "960__a",  # base number
    "961__c",  # CDS modification tag # TODO
    "961__h",  # CDS modification tag # TODO
    "961__l",  # CDS modification tag # TODO
    "961__x",  # CDS modification tag # TODO
    "981__a",  # duplicate record id
    "999C50",
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
}
