from pfs.datamodel import PfsGAObject

class GAPipelineState():
    def __init__(self, id):
        self.id = id

        self.required_product_types = None
        self.product_cache = None

        self.output_product_type = PfsGAObject
        self.output_product = None                    # output product
