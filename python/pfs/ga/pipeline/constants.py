from pfs.datamodel import PfsSingle

class Constants():
    GA_PIPELINE_LOGNAME = 'gapipe'
    
    PFSOBJECT_ID_FORMAT = '{catId:05d}-{tract:05d}-{patch}-{objId:016x}-{nVisit:03d}-0x{pfsVisitHash:016x}'
    PFSARM_ID_FORMAT = '{catId:05d}-{tract:05d}-{patch}-{objId:016x}-{visit:06d}-{arm}{spectrograph:1d}'

    PFSGACONFIG_DIR_FORMAT = 'pfsGAObject/{catId:05d}/{tract:05d}/{patch}'
    PFSGACONFIG_FILENAME_FORMAT = f'pfsGAObject-{PFSOBJECT_ID_FORMAT}.yaml'
    PFSGACONFIG_FILENAME_REGEX = r'pfsGAObject-(?P<catId>\d{5})-(?P<tract>\d{5})-(?P<patch>[0-9a-zA-Z]+)-(?P<objId>[0-9a-fA-F]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-fA-F]{16}).(yaml|json|py)'

    PFSGAOBJECT_DIR_FORMAT = 'pfsGAObject/{catId:05d}/{tract:05d}/{patch}'
    PFSGAOBJECT_FILENAME_FORMAT = f'pfsGAObject-{PFSOBJECT_ID_FORMAT}.fits'
    
    PFSSIGNLE_DIR_FORMAT = 'pfsSingle/{catId:05d}/{tract:05d}/{patch}'
    PFSSINGLE_DIR_GLOB = 'pfsSingle/{catId}/{tract}/{patch}'
    PFSSINGLE_FILENAME_GLOB = 'pfsSingle-{catId}-{tract}-{patch}-{objId}-{visit}.fits'
    PFSSINGLE_FILENAME_REGEX = PfsSingle.filenameRegex

    