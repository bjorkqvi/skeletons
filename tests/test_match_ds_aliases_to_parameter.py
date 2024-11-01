from geo_skeletons.decoders.core_decoders import _match_ds_aliases_to_parameter
import geo_parameters as gp


def test_match_str():
    assert _match_ds_aliases_to_parameter("hs", {"Hm0": "hs"}) == "Hm0"


def test_match_gp():
    assert (
        _match_ds_aliases_to_parameter(gp.wave.Hs("swh"), {"Hm0": gp.wave.Hs}) == "Hm0"
    )


def test_match_gp_name():
    assert _match_ds_aliases_to_parameter(gp.wave.Hs("swh"), {"Hm0": "swh"}) == "Hm0"


def test_no_match():
    assert _match_ds_aliases_to_parameter(gp.wave.Hs("hs"), {"Hm0": "swh"}) is None
