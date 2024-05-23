from pfs.datamodel import PfsSingle

class Constants():
    GA_PIPELINE_LOGNAME = 'gapipe'
    
    PFSGACONFIG_DIR_FORMAT = 'pfsGAObject/{catId}/{tract}/{patch}'
    PFSGACONFIG_FILENAME_FORMAT = 'pfsGAConfig-{catId:05d}-{tract:05d}-{patch}-{objId:016x}-{nVisit:03d}-0x{pfsVisitHash:016x}.yaml'
    
    PFSSINGLE_DIR_GLOB = 'pfsSingle/{catId}/{tract}/{patch}'
    PFSSINGLE_FILENAME_GLOB = 'pfsSingle-{catId}-{tract}-{patch}-{objId}-{visit}.fits'
    PFSSINGLE_FILENAME_REGEX = PfsSingle.filenameRegex

    