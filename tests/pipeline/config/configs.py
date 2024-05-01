# TODO: update this
TEST_CONFIG = dict(
    objId = 1,
    visit = [
        98765,
        98766,
        98767
    ],
    catId = {
        98765: 90003,
        98766: 90003,
        98767: 90003,
    },
    tract = {
        98765: 1,
        98766: 1,
        98767: 1,
    },
    patch = {
        98765: '1,1',
        98766: '1,1',
        98767: '1,1',
    },
    designId = {
        98765: 7884270544754596914,
        98766: 7884270544754596914,
        98767: 7884270544754596914,
    },
    date = {
        98765: '2024-01-03',
        98766: '2024-01-03',
        98767: '2024-01-03',
    },
    fiber = {
        98765: '2024-01-03',
        98766: '2024-01-03',
        98767: '2024-01-03',
    }
)

TEST_CONFIG_EDR2_90006 = dict(
    object = dict(
        objId = 36072,
        catId = 90006,
        tract = 1,
        patch = '1,1',
        visits = {
            97821: dict(
                date = '2023-07-24',
                pfsDesignId = 0x66f04f565a40eca5,
                fiberId = 640
            ),
            98408: dict(
                date = '2023-07-26',
                pfsDesignId = 0x25d33a63c51002d5,
                fiberId = 640
            )
        },
    ),
    rvfit = dict(
        fit_arms = [ 'b', 'm', 'n' ],
        rvfit_args = dict(
            flux_corr_per_arm = True,
            flux_corr_per_exp = False,
            amplitude_per_arm = True,
            amplitude_per_exp = True,
        )
    ),
)