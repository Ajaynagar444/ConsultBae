"""Pure normalisation functions. No database.

Every case here is a value that actually appears in the three CSVs, or a
deliberate near-miss that must be rejected rather than guessed at.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.pipeline import normalize as nz


# ---------------------------------------------------------------------------
# email
# ---------------------------------------------------------------------------

def test_email_lowercased():
    r = nz.normalize_email("ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG")
    assert r.value == "isha.chopra95@mailtest.example.org"
    assert "email_case_normalised" in r.issues


def test_email_already_clean_reports_nothing():
    r = nz.normalize_email("tanvi.gupta31@example.com")
    assert r.value == "tanvi.gupta31@example.com"
    assert r.issues == ()


def test_email_whitespace_trimmed():
    r = nz.normalize_email("  a.b@example.com  ")
    assert r.value == "a.b@example.com"
    assert "email_whitespace_trimmed" in r.issues


def test_email_original_is_preserved():
    r = nz.normalize_email("ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG")
    assert r.original == "ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG"


def test_email_rejects_the_shifted_rows_skill_list():
    """The corrupted row puts a skill list where the address should be."""
    r = nz.normalize_email("react, javascript, mysql")
    assert r.value is None
    assert "email_invalid" in r.issues


@pytest.mark.parametrize("bad", ["", "   ", None, "no-at-sign", "a@b", "a@@b.com"])
def test_email_rejects_junk(bad):
    assert nz.normalize_email(bad).value is None


# ---------------------------------------------------------------------------
# phone - every observed format
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", [
    "+919000000254",   # source1
    "9000000254",      # source1 plain
    "09000000254",     # source1 trunk prefix
    "919000000254",    # source3
    "+91-9000000254",  # source3 hyphenated
    " 9000000254 ",    # incidental whitespace
])
def test_phone_all_observed_formats_fold_to_one_value(raw):
    r = nz.normalize_phone(raw)
    assert r.ok, raw
    assert r.value.national == "9000000254"
    assert r.value.e164 == "+919000000254"


def test_phone_plain_ten_digits_needs_no_change():
    assert nz.normalize_phone("9000000254").issues == ()


def test_phone_prefixed_is_flagged_as_reformatted():
    assert "phone_format_normalised" in nz.normalize_phone("+91-9000000254").issues


@pytest.mark.parametrize("bad", [
    "",              # blank
    "Phone Number",  # the embedded header row
    "12345",         # too short
    "1000000000",    # Indian mobiles do not start with 1
    "5000000000",    # nor 5
    "99999999999999",
])
def test_phone_rejects_rather_than_guesses(bad):
    r = nz.normalize_phone(bad)
    assert r.value is None
    assert r.issues


# ---------------------------------------------------------------------------
# name
# ---------------------------------------------------------------------------

def test_name_trimmed_and_collapsed():
    r = nz.normalize_name("  Rohit   Verma ")
    assert r.value.display == "Rohit Verma"
    assert r.value.key == "rohit verma"


def test_name_casing_is_preserved_for_display():
    """Choosing a display form is survivorship's job, not staging's."""
    r = nz.normalize_name("RITU SHARMA")
    assert r.value.display == "RITU SHARMA"
    assert r.value.key == "ritu sharma"


def test_r_verma_is_not_expanded_into_rohit_verma():
    """The single most important non-decision in this module.

    source1 lines 25 and 31 are the same person, but the evidence is a shared
    email and phone. Expanding `R.` to `Rohit` would manufacture that identity
    claim from nothing.
    """
    abbreviated = nz.normalize_name("R. Verma")
    full = nz.normalize_name("Rohit Verma")

    assert abbreviated.value.display == "R. Verma"
    assert abbreviated.value.key == "r verma"
    assert full.value.key == "rohit verma"
    assert abbreviated.value.key != full.value.key
    assert "name_abbreviated" in abbreviated.issues


def test_name_key_strips_punctuation_without_joining_words():
    assert nz.normalize_name("R. Verma").value.key == "r verma"


def test_distinct_people_sharing_a_first_name_keep_distinct_keys():
    assert nz.normalize_name("Deepak Nair").value.key == nz.normalize_name("DEEPAK NAIR").value.key
    assert nz.normalize_name("Deepak Nair").value.key != nz.normalize_name("Deepak Mehta").value.key


@pytest.mark.parametrize("bad", ["", "   ", None, "..."])
def test_name_rejects_empty(bad):
    assert nz.normalize_name(bad).value is None


# ---------------------------------------------------------------------------
# city
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["Bangalore", "bangalore", "Bengaluru", "BENGALURU"])
def test_bangalore_folds_to_bengaluru(raw):
    assert nz.normalize_city(raw).value.name == "Bengaluru"


@pytest.mark.parametrize("raw", ["Gurgaon", "GURGAON", "Gurugram", "gurugram "])
def test_gurgaon_folds_to_gurugram(raw):
    assert nz.normalize_city(raw).value.name == "Gurugram"


def test_trailing_space_is_trimmed_and_reported():
    r = nz.normalize_city("gurugram ")
    assert r.value.name == "Gurugram"
    assert "city_whitespace_trimmed" in r.issues


def test_delhi_ncr_stays_a_region():
    """Collapsing it to Delhi, Noida or Gurugram would silently relocate people."""
    r = nz.normalize_city("Delhi NCR")
    assert r.value.name == "Delhi NCR"
    assert r.value.is_region is True
    assert "city_region_preserved" in r.issues


def test_delhi_and_new_delhi_stay_distinct():
    assert nz.normalize_city("Delhi").value.name == "Delhi"
    assert nz.normalize_city("new delhi").value.name == "New Delhi"


def test_delhi_is_not_a_region():
    assert nz.normalize_city("Delhi").value.is_region is False


@pytest.mark.parametrize("raw,expected", [("NOIDA", "Noida"), ("Noida ", "Noida"), ("pune", "Pune")])
def test_casing_folded(raw, expected):
    assert nz.normalize_city(raw).value.name == expected


def test_unknown_city_is_kept_not_guessed():
    r = nz.normalize_city("Atlantis")
    assert r.value.name == "Atlantis"
    assert "city_unknown" in r.issues


# ---------------------------------------------------------------------------
# CTC
# ---------------------------------------------------------------------------

def test_absolute_rupees_pass_through():
    amount, unit = nz.normalize_ctc("417964").value
    assert (amount, unit) == (417964, "rupee")


def test_lakhs_are_converted():
    amount, unit = nz.normalize_ctc("4.2").value
    assert (amount, unit) == (420000, "lakh")


def test_lakh_boundary_uses_the_documented_rule():
    assert nz.normalize_ctc("11.9").value == (1190000, "lakh")
    assert nz.normalize_ctc("99").value[1] == "lakh"
    assert nz.normalize_ctc("100").value[1] == "rupee"


def test_the_two_observed_ranges_do_not_collide():
    """Max lakh value and min rupee value must land on opposite sides."""
    assert nz.normalize_ctc("11.9").value[1] == "lakh"
    assert nz.normalize_ctc("327287").value[1] == "rupee"


def test_source_unit_is_recorded_so_the_conversion_is_auditable():
    assert nz.normalize_ctc("4.2").value[1] == "lakh"
    assert "ctc_unit_lakh" in nz.normalize_ctc("4.2").issues


@pytest.mark.parametrize("bad", ["", "abc", "-5", "0"])
def test_ctc_rejects_junk(bad):
    assert nz.normalize_ctc(bad).value is None


# ---------------------------------------------------------------------------
# rate
# ---------------------------------------------------------------------------

def test_hourly_rate():
    assert nz.normalize_rate("1415/hr").value == (1415.0, "per_hour")


def test_monthly_rate_expands_k_but_keeps_the_unit():
    assert nz.normalize_rate("15k/month").value == (15000.0, "per_month")


def test_monthly_is_never_converted_to_hourly():
    """The two scales do not reconcile; any factor would be fabricated."""
    amount, unit = nz.normalize_rate("15k/month").value
    assert unit == "per_month"
    assert amount == 15000.0          # not divided by any hours-per-month figure


def test_observed_extremes_parse():
    assert nz.normalize_rate("330/hr").value == (330.0, "per_hour")
    assert nz.normalize_rate("1483/hr").value == (1483.0, "per_hour")
    assert nz.normalize_rate("79k/month").value == (79000.0, "per_month")


@pytest.mark.parametrize("bad", ["", "Isha Chopra", "1406", "1406/day", "abc/hr"])
def test_rate_rejects_unrecognised(bad):
    assert nz.normalize_rate(bad).value is None


# ---------------------------------------------------------------------------
# applied date - all four observed formats
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("2026-08-08", date(2026, 8, 8)),    # YYYY-MM-DD
    ("24-07-2026", date(2026, 7, 24)),   # DD-MM-YYYY
    ("07/13/2026", date(2026, 7, 13)),   # MM/DD/YYYY
    ("7 Jul 2026", date(2026, 7, 7)),    # D Mon YYYY
])
def test_all_four_formats(raw, expected):
    assert nz.normalize_date(raw).value == expected


def test_dash_and_slash_are_read_differently_on_purpose():
    """Disambiguated by evidence across the column, not by assumption.

    Slash values include 07/13 and 08/21, so day > 12 proves MM/DD.
    Dash values include 21-08 and 28-07, so first > 12 proves DD-MM.

    Both strings below happen to denote 3 July 2026, reached by opposite
    readings of the same two digits: `03-07` is day-then-month, `07/03` is
    month-then-day. Read with the wrong rule, each would land four months away.
    """
    assert nz.normalize_date("03-07-2026").value == date(2026, 7, 3)   # DD-MM: 3 July
    assert nz.normalize_date("07/03/2026").value == date(2026, 7, 3)   # MM/DD: 3 July


def test_swapping_the_two_rules_would_change_the_answer():
    """Guards the claim above: the formats are genuinely not interchangeable."""
    assert nz.normalize_date("04-05-2026").value == date(2026, 5, 4)   # DD-MM: 4 May
    assert nz.normalize_date("04/05/2026").value == date(2026, 4, 5)   # MM/DD: 5 April


def test_the_values_that_forced_each_rule():
    assert nz.normalize_date("07/13/2026").value == date(2026, 7, 13)   # 13 cannot be a month
    assert nz.normalize_date("21-08-2026").value == date(2026, 8, 21)   # 21 cannot be a month


def test_format_label_is_recorded():
    assert "date_format_mm_dd_yyyy" in nz.normalize_date("08/21/2026").issues
    assert "date_format_d_mon_yyyy" in nz.normalize_date("2 Jul 2026").issues


@pytest.mark.parametrize("bad", ["", "not a date", "31-02-2026", "13/13/2026", "2026/08/08"])
def test_date_refuses_to_guess(bad):
    r = nz.normalize_date(bad)
    assert r.value is None
    assert r.issues


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["Active", "active", "ACTIVE", " active "])
def test_status_active_variants(raw):
    assert nz.normalize_status(raw).value == "active"


def test_status_inactive_and_paused():
    assert nz.normalize_status("Inactive").value == "inactive"
    assert nz.normalize_status("paused").value == "paused"


def test_corrupted_status_from_the_shifted_row_is_refused():
    """`Pune` sits in the status column before the row is repaired."""
    r = nz.normalize_status("Pune")
    assert r.value is None
    assert "status_unrecognised" in r.issues


# ---------------------------------------------------------------------------
# verified
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["Y", "Yes", "yes", "YES"])
def test_verified_true(raw):
    assert nz.normalize_verified(raw).value is True


@pytest.mark.parametrize("raw", ["N", "No", "no"])
def test_verified_false(raw):
    assert nz.normalize_verified(raw).value is False


def test_verified_header_value_is_refused():
    """`Verified` is what the embedded header row puts in this column."""
    assert nz.normalize_verified("Verified").value is None


def test_verified_distinguishes_false_from_missing():
    assert nz.normalize_verified("No").value is False
    assert nz.normalize_verified("").value is None


# ---------------------------------------------------------------------------
# skills
# ---------------------------------------------------------------------------

def test_skills_lowercased_and_split():
    r = nz.normalize_skills("n8n, LangChain, REST APIs, MongoDB, SQL")
    assert r.value == ("n8n", "langchain", "rest apis", "mongodb", "sql")


def test_source1_and_source2_agree_once_folded():
    """Verified during profiling for all 15 overlapping people."""
    a = nz.normalize_skills("Selenium, Web Scraping, React, Docker, SQL, FastAPI").value
    b = nz.normalize_skills("selenium, web scraping, react, docker, sql, fastapi").value
    assert a == b


def test_skills_deduplicated():
    r = nz.normalize_skills("SQL, sql, Python")
    assert r.value == ("sql", "python")
    assert "skills_duplicates_removed" in r.issues


def test_skills_order_of_first_appearance_is_kept():
    assert nz.normalize_skills("SQL, Python, Docker").value == ("sql", "python", "docker")


def test_every_observed_token_is_in_the_canonical_vocabulary():
    r = nz.normalize_skills(
        "Docker, FastAPI, JavaScript, LangChain, MongoDB, MySQL, Pandas, "
        "Python, REST APIs, React, SQL, Selenium, Web Scraping, Zapier, n8n"
    )
    assert set(r.value) == set(nz.CANONICAL_SKILLS)
    assert "skills_unknown_token" not in r.issues


def test_unknown_token_is_kept_and_flagged_not_invented():
    """`active` reaches this column only via the un-repaired shifted row."""
    r = nz.normalize_skills("active")
    assert r.value == ("active",)
    assert "skills_unknown_token" in r.issues


def test_no_skill_is_invented():
    assert len(nz.CANONICAL_SKILLS) == 15
    assert nz.normalize_skills("").value == ()


# ---------------------------------------------------------------------------
# numerics
# ---------------------------------------------------------------------------

def test_experience_range():
    assert nz.normalize_experience("4.2").value == 4.2
    assert nz.normalize_experience("0.8").value == 0.8
    assert nz.normalize_experience("-1").value is None


def test_zero_projects_is_a_real_value_not_a_null():
    """source3 line 9 legitimately has 0."""
    r = nz.normalize_projects("0")
    assert r.value == 0
    assert r.ok is False or r.value == 0     # 0 is falsy; must not be treated as missing
    assert nz.normalize_projects("").value is None
