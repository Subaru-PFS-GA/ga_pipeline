from pfs.datamodel import PfsSingle

class Constants():
    GA_PIPELINE_LOGNAME = 'gapipe'
    
    PFSOBJECT_ID_FORMAT = '{catId:05d}-{tract:05d}-{patch}-{objId:016x}-{nVisit:03d}-0x{pfsVisitHash:016x}'
    PFSARM_ID_FORMAT = '{catId:05d}-{tract:05d}-{patch}-{objId:016x}-{visit:06d}-{arm}{spectrograph:1d}'

    PFSGACONFIG_DIR_FORMAT = 'pfsGAObject/{catId:05d}/{tract:05d}/{patch}'
    PFSGACONFIG_FILENAME_FORMAT = f'pfsGAObject-{PFSOBJECT_ID_FORMAT}.yaml'
    PFSGACONFIG_FILENAME_REGEX = r'pfsGAObject-(?P<catId>\d{5})-(?P<tract>\d{5})-(?P<patch>[0-9a-zA-Z]+)-(?P<objId>[0-9a-fA-F]{16})-(?P<nVisit>\d{3})-0x(?P<pfsVisitHash>[0-9a-fA-F]{16})\.(yaml|json|py)$'

    PFSGAOBJECT_DIR_FORMAT = 'pfsGAObject/{catId:05d}/{tract:05d}/{patch}'
    PFSGAOBJECT_FILENAME_FORMAT = f'pfsGAObject-{PFSOBJECT_ID_FORMAT}.fits'

    PFSDESIGN_DIR_FORMAT = 'pfsDesign'
    PFSDESIGN_DIR_GLOB = 'pfsDesign'
    PFSDESIGN_FILENAME_GLOB = 'pfsDesign-0x{pfsDesignId}.fits'
    PFSDESIGN_FILENAME_FORMAT = 'pfsDesign-0x{pfsDesignId:016x}.fits'
    PFSDESIGN_FILENAME_REGEX = r'pfsDesign-0x(?P<pfsDesignId>[0-9a-fA-F]{16})\.(?:fits|fits\.gz)$'

    PFSCONFIG_DIR_FORMAT = 'pfsConfig/{date:%Y-%m-%d}/'
    PFSCONFIG_DIR_GLOB = 'pfsConfig/**/'
    PFSCONFIG_FILENAME_GLOB = 'pfsConfig-0x{pfsDesignId}-{visit}.fits'
    PFSCONFIG_FILENAME_FORMAT = 'pfsConfig-0x{pfsDesignId:016x}-{visit:06d}.fits'
    PFSCONFIG_FILENAME_REGEX = r'pfsConfig-0x(?P<pfsDesignId>[0-9a-fA-F]{16})-(?P<visit>\d{6})\.(fits|fits\.gz)$'
    PFSCONFIG_PATH_REGEX = r'(?P<date>\d{4}-\d{2}-\d{2})/' + PFSCONFIG_FILENAME_REGEX
    
    PFSSINGLE_DIR_FORMAT = 'pfsSingle/{catId:05d}/{tract:05d}/{patch}'
    PFSSINGLE_DIR_GLOB = 'pfsSingle/{catId}/{tract}/{patch}'
    PFSSINGLE_FILENAME_GLOB = 'pfsSingle-{catId}-{tract}-{patch}-{objId}-{visit}.fits'
    PFSSINGLE_FILENAME_REGEX = r'pfsSingle-(?P<catId>\d{5})-(?P<tract>\d{5})-(?P<patch>.*)-(?P<objId>[0-9a-f]{16})-(?P<visit>\d{6})\.(fits|fits\.gz)$'

    