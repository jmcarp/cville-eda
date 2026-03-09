# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pydantic",
#     "pytest",
#     "pyyaml",
# ]
# ///
"""Tests for unit count extraction from description text."""

import pytest

from build_projects import extract_units, extract_lots, extract_density


class TestExtractUnits:
    """Test cases for extract_units()."""

    # -- Basic matches --

    def test_generic_units(self):
        assert extract_units("525 units") == (525, "generic")

    def test_dwelling_units(self):
        assert extract_units("48 dwelling units") == (48, "dwelling")

    def test_residential_units(self):
        assert extract_units("120 residential units") == (120, "dwelling")

    def test_apartment_units(self):
        assert extract_units("318 apartment units") == (318, "housing_type")

    def test_townhome_units(self):
        assert extract_units("72 townhome units") == (72, "housing_type")

    def test_townhouse_units(self):
        assert extract_units("50 townhouse units") == (50, "housing_type")

    def test_condo_units(self):
        assert extract_units("30 condo units") == (30, "housing_type")

    # -- Priority: more specific patterns match first --

    def test_dwelling_before_generic(self):
        count, match_type = extract_units("100 dwelling units and 50 units of storage")
        assert count == 100
        assert match_type == "dwelling"

    def test_apartment_before_generic(self):
        count, match_type = extract_units("200 apartment units on 10 units of land")
        assert count == 200
        assert match_type == "housing_type"

    # -- Per-acre exclusion (the 600 Rio bug) --

    def test_units_per_acre_excluded(self):
        assert extract_units("48 units per acre") == (None, None)

    def test_dwelling_units_per_acre_excluded(self):
        assert extract_units("48 dwelling units per acre") == (None, None)

    def test_units_slash_acre_excluded(self):
        assert extract_units("48 units/acre") == (None, None)

    def test_per_acre_falls_through_to_total(self):
        """The 600 Rio case: 'maximum of 153 units' appears before '48 dwelling units per acre'."""
        desc = (
            "for a maximum of 153 units on the property. "
            "The applicant proposes to establish a Code of Development (COD) "
            "that will permit up to 48 dwelling units per acre"
        )
        assert extract_units(desc) == (153, "generic")

    def test_density_then_total_units(self):
        """999 Rio Road: density mentioned before total count."""
        desc = (
            "for a total of 38 units, on a parcel of 1.918 acres, "
            "at a density of approximately 20 dwelling units per acre"
        )
        assert extract_units(desc) == (38, "generic")

    def test_multiple_densities_no_total(self):
        """Description with only density figures should return None."""
        desc = "residential density is 12 dwelling units per acre; 18 dwelling units per acre in a mixed use setting"
        assert extract_units(desc) == (None, None)

    # -- Other exclusions --

    def test_sq_before_excluded(self):
        assert extract_units("1000 sq units") == (None, None)

    def test_square_before_excluded(self):
        assert extract_units("500 square units") == (None, None)

    def test_dollar_before_excluded(self):
        assert extract_units("costs $50 units") == (None, None)

    def test_acre_before_excluded(self):
        assert extract_units("per acre 6 units") == (None, None)

    def test_sf_after_excluded(self):
        assert extract_units("48 units sf") == (None, None)

    def test_square_feet_after_excluded(self):
        assert extract_units("100 units square feet") == (None, None)

    # -- Comma-formatted numbers --

    def test_comma_number_parsed(self):
        """'1,600 units' parses as 1600 — commas are stripped."""
        assert extract_units("ranges from 1,600 units to 2,200 units") == (1600, "generic")

    def test_comma_number_not_false_positive(self):
        """A real count after a comma-formatted number should still match."""
        desc = "a 1,600-acre property with 200 units"
        assert extract_units(desc) == (200, "generic")

    def test_comma_number_in_range(self):
        """A comma-formatted number within range should parse correctly."""
        assert extract_units("a total of 1,500 units") == (1500, "generic")

    # -- Range/bounds validation --

    def test_single_unit_rejected(self):
        assert extract_units("1 unit") == (None, None)

    def test_two_units_accepted(self):
        assert extract_units("2 units") == (2, "generic")

    def test_4000_units_accepted(self):
        assert extract_units("4000 units") == (4000, "generic")

    def test_over_4000_rejected(self):
        assert extract_units("4001 units") == (None, None)

    # -- Empty/missing input --

    def test_empty_string(self):
        assert extract_units("") == (None, None)

    def test_none_input(self):
        assert extract_units(None) == (None, None)

    def test_no_units_in_text(self):
        assert extract_units("Request for rezoning of 5 parcels") == (None, None)

    # -- Real-world descriptions --

    def test_holly_hills(self):
        """SDP202400042: 318 apartment + 72 townhouse, at 12.65 dwelling units per acre."""
        desc = (
            "Initial site plan for 318 apartment units and 72 townhouse units "
            "for a total of 12.65 dwelling units per acre"
        )
        assert extract_units(desc) == (318, "housing_type")

    def test_granger_development(self):
        """ZMA202300010: 203 units with density of 5.98 dwelling units per acre."""
        desc = (
            "The proposal is for 203 units, a mixture of single family detached "
            "and single family attached housing units, at a net density of "
            "5.98 dwelling units per acre"
        )
        assert extract_units(desc) == (203, "generic")

    def test_pen_place(self):
        """SUB202300064: 15 lots at 3.61 units per acre."""
        desc = (
            "Preliminary plat proposing the subdivision of 15 attached "
            "townhouse unit lots at a proposed density of 3.61 units per acre"
        )
        # "15 attached townhouse unit" — "unit" is singular after "townhouse",
        # but the lot pattern should not interfere
        result = extract_units(desc)
        # 15 doesn't match "units" (singular "unit lots" not "units")
        # but 61 from "3.61" shouldn't match either due to the decimal
        assert result[0] != 61 if result[0] is not None else True

    # -- Real-world verified cases (known correct counts) --

    def test_600_rio(self):
        """ZMA-2025-00001: 153 units total, not 48 dwelling units per acre."""
        desc = (
            "construct several new multi-family and two family buildings "
            "for a maximum of 153 units on the property. "
            "The applicant proposes to establish a Code of Development (COD) "
            "that will permit up to 48 dwelling units per acre"
        )
        assert extract_units(desc) == (153, "generic")

    def test_old_ivy_residences(self):
        """ZMA202100008: 525 residential units."""
        desc = (
            "Rezone multiple properties to the R15 Zoning District, "
            "and amend existing proffers, to allow a maximum of "
            "525 residential units (14 units/acre)"
        )
        assert extract_units(desc) == (525, "dwelling")

    def test_holly_hills_zma(self):
        """ZMA202300012: 410 dwelling units across the site."""
        desc = (
            "rezone approximately 51.55 acres to a Neighborhood Model District (NMD) "
            "to allow for up to 410 dwelling units"
        )
        assert extract_units(desc) == (410, "dwelling")

    def test_rio_point(self):
        """SDP202200009: 327 dwelling units at 11.97 du/acre."""
        desc = (
            "Initial site plan proposal to develop two properties "
            "measuring 27.71 total acres for multifamily and single-family "
            "attached dwelling units. A total of 327 dwelling units "
            "are proposed at a density of 11.97 du/acre"
        )
        assert extract_units(desc) == (327, "dwelling")

    def test_north_fork_discovery_park(self):
        """ZMA202100016: 1,400 units in mixed-use development."""
        desc = (
            "approximately 1,400 units of single family, multifamily, "
            "and mixed-use residential, along with commercial and "
            "institutional uses"
        )
        assert extract_units(desc) == (1400, "generic")

    def test_southwood_phase_2(self):
        """ZMA202100013: 1,000 units of affordable housing."""
        desc = (
            "Rezone property from residential to NMD to allow "
            "up to 1,000 units of single-family and multi-family housing"
        )
        assert extract_units(desc) == (1000, "generic")

    def test_old_trail_village(self):
        """ZMA200400024: 1,600 to 2,200 residential units."""
        desc = (
            "a combination of residential and commercial uses "
            "ranging from 1,600 units to 2,200 units"
        )
        assert extract_units(desc) == (1600, "generic")

    def test_multifamily_units(self):
        """Multifamily pattern should match."""
        assert extract_units("70 multifamily units") == (70, "housing_type")

    def test_multi_family_units(self):
        """Multi-family (hyphenated) pattern should match."""
        assert extract_units("70 multi-family units") == (70, "housing_type")


class TestExtractLots:
    """Test cases for extract_lots()."""

    def test_basic_lots(self):
        assert extract_lots("subdivision into 15 lots") == 15

    def test_parcels(self):
        assert extract_lots("creating 88 parcels of land") == 88

    def test_single_lot_rejected(self):
        assert extract_lots("1 lot") is None

    def test_comma_number_parsed(self):
        assert extract_lots("on 1,200 lots") == 1200

    def test_empty(self):
        assert extract_lots("") is None

    def test_none(self):
        assert extract_lots(None) is None


class TestExtractDensity:
    """Test cases for extract_density()."""

    def test_dwelling_units_per_acre(self):
        assert extract_density("48 dwelling units per acre") == 48.0

    def test_du_per_acre(self):
        assert extract_density("11.97 du/acre") == 11.97

    def test_units_per_acre(self):
        assert extract_density("3.61 units per acre") == 3.61

    def test_decimal_density(self):
        assert extract_density("at a density of 5.98 dwelling units per acre") == 5.98

    def test_takes_highest(self):
        """Multiple densities: returns the highest."""
        desc = "12 dwelling units per acre; 18 dwelling units per acre in mixed use"
        assert extract_density(desc) == 18.0

    def test_rio_point(self):
        """327 dwelling units at 11.97 du/acre."""
        desc = (
            "A total of 327 dwelling units "
            "are proposed at a density of 11.97 du/acre"
        )
        assert extract_density(desc) == 11.97

    def test_no_density(self):
        assert extract_density("Request for rezoning of 5 parcels") is None

    def test_empty(self):
        assert extract_density("") is None

    def test_none(self):
        assert extract_density(None) is None
