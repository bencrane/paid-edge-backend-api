"""LinkedIn targeting criteria builder and campaign objective validation."""

# --- Facet URN mappings ---

FACET_LOCATIONS = "urn:li:adTargetingFacet:locations"
FACET_INTERFACE_LOCALES = "urn:li:adTargetingFacet:interfaceLocales"
FACET_INDUSTRIES = "urn:li:adTargetingFacet:industries"
FACET_SENIORITIES = "urn:li:adTargetingFacet:seniorities"
FACET_JOB_FUNCTIONS = "urn:li:adTargetingFacet:jobFunctions"
FACET_STAFF_COUNT_RANGES = "urn:li:adTargetingFacet:staffCountRanges"
FACET_MATCHED_AUDIENCES = "urn:li:adTargetingFacet:matchedAudiences"
FACET_EMPLOYERS = "urn:li:adTargetingFacet:employers"

DEFAULT_LOCALE = "urn:li:locale:en_US"

# --- Objective validation ---

VALID_OBJECTIVES: dict[str, dict] = {
    "BRAND_AWARENESS": {
        "types": ["SPONSORED_UPDATES", "DYNAMIC"],
        "cost_types": ["CPM"],
    },
    "WEBSITE_VISITS": {
        "types": [
            "SPONSORED_UPDATES",
            "TEXT_AD",
            "DYNAMIC",
            "SPONSORED_INMAILS",
        ],
        "cost_types": ["CPC", "CPM"],
    },
    "ENGAGEMENT": {
        "types": ["SPONSORED_UPDATES"],
        "cost_types": ["CPC", "CPM"],
    },
    "VIDEO_VIEWS": {
        "types": ["SPONSORED_UPDATES"],
        "cost_types": ["CPV"],
    },
    "LEAD_GENERATION": {
        "types": ["SPONSORED_UPDATES", "SPONSORED_INMAILS"],
        "cost_types": ["CPC", "CPM"],
        "offsite_blocked": True,
    },
    "WEBSITE_CONVERSIONS": {
        "types": ["SPONSORED_UPDATES", "TEXT_AD", "DYNAMIC"],
        "cost_types": ["CPC", "CPM"],
    },
    "JOB_APPLICANTS": {
        "types": ["SPONSORED_UPDATES", "DYNAMIC"],
        "cost_types": ["CPC", "CPM"],
    },
    "TALENT_LEADS": {
        "types": ["SPONSORED_UPDATES", "DYNAMIC"],
        "cost_types": ["CPC", "CPM"],
    },
}


class InvalidCampaignConfigError(ValueError):
    """Raised when campaign objective/type/cost_type combination is invalid."""


def validate_campaign_config(
    objective: str,
    campaign_type: str,
    cost_type: str,
    offsite_delivery: bool = False,
) -> None:
    """Validate objective + type + cost_type combination.

    Raises InvalidCampaignConfigError if the combination is not allowed.
    """
    if objective not in VALID_OBJECTIVES:
        raise InvalidCampaignConfigError(
            f"Unknown objective '{objective}'. "
            f"Valid: {list(VALID_OBJECTIVES.keys())}"
        )

    spec = VALID_OBJECTIVES[objective]

    if campaign_type not in spec["types"]:
        raise InvalidCampaignConfigError(
            f"Campaign type '{campaign_type}' not valid for objective "
            f"'{objective}'. Valid types: {spec['types']}"
        )

    if cost_type not in spec["cost_types"]:
        raise InvalidCampaignConfigError(
            f"Cost type '{cost_type}' not valid for objective "
            f"'{objective}'. Valid cost types: {spec['cost_types']}"
        )

    if spec.get("offsite_blocked") and offsite_delivery:
        raise InvalidCampaignConfigError(
            f"Offsite delivery must be disabled for objective "
            f"'{objective}'."
        )


# --- Targeting criteria builder ---


def build_targeting_criteria(
    locations: list[str] | None = None,
    locales: list[str] | None = None,
    industries: list[str] | None = None,
    seniorities: list[str] | None = None,
    job_functions: list[str] | None = None,
    company_sizes: list[str] | None = None,
    matched_audiences: list[str] | None = None,
    exclude_companies: list[str] | None = None,
) -> dict:
    """Build LinkedIn AND/OR boolean targeting structure.

    Each facet becomes an OR clause (any value matches).
    All facets are ANDed together.
    Exclusions go in a separate `exclude` block.
    interfaceLocales is always included (LinkedIn undocumented requirement).
    """
    and_clauses: list[dict] = []

    # interfaceLocales MUST always be included
    locale_values = locales if locales else [DEFAULT_LOCALE]
    and_clauses.append({"or": {FACET_INTERFACE_LOCALES: locale_values}})

    facet_map = [
        (locations, FACET_LOCATIONS),
        (industries, FACET_INDUSTRIES),
        (seniorities, FACET_SENIORITIES),
        (job_functions, FACET_JOB_FUNCTIONS),
        (company_sizes, FACET_STAFF_COUNT_RANGES),
        (matched_audiences, FACET_MATCHED_AUDIENCES),
    ]

    for values, facet_urn in facet_map:
        if values:
            and_clauses.append({"or": {facet_urn: values}})

    result: dict = {"include": {"and": and_clauses}}

    # Exclusions in separate exclude block
    if exclude_companies:
        result["exclude"] = {
            "or": {FACET_EMPLOYERS: exclude_companies}
        }

    return result
