from watchgod import  Change


watchconfig = [
    {
        "dir":  "/var/spool/gisc/incoming",
        "type": "BUFR",
        "trigger": [Change.added,  Change.modified],
        "pattern": r"^.*.zip$",
        "action": process,
        "action_args":  {
            "loglevel":  logging.DEBUG,
        },
        "cleanup":  cleanup,
        "cleanup_every":  3600,
        "cleanup_args":  {
            "todir": "/var/spool/gisc/processed",
            "age":  86400 * 7,
        },
    },
    {
        "dir": "/var/spool/madis",
        "type": "netCDF",
        "trigger": [Change.added,  Change.modified],
        "pattern": r"^.*.gz$",
        "action": process,
        "action_args":  {
            "loglevel":  logging.INFO,
        },
        "cleanup": cleanup,
        "cleanup_every":  3600,
        "cleanup_args":  {
            "todir": "/var/spool/madis-processed",
            "age":  86400 * 7,
        },
    },
]
