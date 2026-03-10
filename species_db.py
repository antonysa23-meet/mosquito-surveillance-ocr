"""
Harris County mosquito species database.
Canonical abbreviation -> full scientific name.
Source: Texas DSHS / CDC surveillance abbreviation standards.
Delete this file and MODELS.md to fully remove the local DB from the project.
"""

SPECIES_DB: dict[str, str] = {
    "Cx.qf": "Culex quinquefasciatus",
    "Cx.ng": "Culex nigripalpus",
    "Cx.sa": "Culex salinarius",
    "Cx.re": "Culex restuans",
    "Cx.er": "Culex erraticus",
    "Cx.pi": "Culex pipiens",
    "Cx.ta": "Culex tarsalis",
    "Cx.co": "Culex coronator",
    "Ae.ab": "Aedes albopictus",
    "Ae.ae": "Aedes aegypti",
    "Ae.ve": "Aedes vexans",
    "Ae.at": "Aedes atlanticus",
    "Ae.in": "Aedes infirmatus",
    "Ae.mi": "Aedes mitchellae",
    "Ae.st": "Aedes sticticus",
    "Ae.tr": "Aedes triseriatus",
    "Oc.tr": "Ochlerotatus triseriatus",
    "Oc.in": "Ochlerotatus infirmatus",
    "Ps.co": "Psorophora columbiae",
    "Ps.fx": "Psorophora ferox",
    "Ps.ci": "Psorophora ciliata",
    "Ps.ho": "Psorophora howardii",
    "Ps.di": "Psorophora discolor",
    "Ps.ob": "Psorophora obtusipes",
    "An.cr": "Anopheles crucians",
    "An.qu": "Anopheles quadrimaculatus",
    "An.at": "Anopheles atropos",
    "An.ps": "Anopheles pseudopunctipennis",
    "Ma.dy": "Mansonia dyari",
    "Ma.ti": "Mansonia titillans",
    "Cq.pe": "Coquillettidia perturbans",
    "Ur.sa": "Uranotaenia sapphirina",
    "Or.si": "Orthopodomyia signifera",
    "Tx.ru": "Toxorhynchites rutilus",
    "Wy.sm": "Wyeomyia smithii",
    "De.ca": "Deinocerites cancer",
    "Cu.ng": "Culex nigripalpus",
    "He.ab": "Aedes albopictus",
}

# Direct variant -> canonical map (checked before fuzzy matching).
# Covers common single-character OCR misreads (0/O, l/1, rn/m, F/P, etc.)
ABBREV_VARIANTS: dict[str, str] = {
    # Culex quinquefasciatus (most common Harris County species)
    "Cxqf": "Cx.qf", "Cxgf": "Cx.qf", "Cx.gf": "Cx.qf",
    "cx.qf": "Cx.qf", "cxqf": "Cx.qf", "CXqf": "Cx.qf",
    "Cx.of": "Cx.qf",  # 'q' misread as 'o'
    "Cx.9f": "Cx.qf",  # 'q' misread as '9'
    "Cxaf": "Cx.qf",   # 'q' misread as 'a'
    "Cx.af": "Cx.qf",
    "Cvctl": "Cx.qf",  # EasyOCR specific garble seen in test run
    "6Cvctl": "Cx.qf",
    "Cx.qt": "Cx.qf",  # 'f' misread as 't'
    "Cx.qF": "Cx.qf",
    # Aedes albopictus
    "Aeab": "Ae.ab", "Aeob": "Ae.ab", "Heab": "Ae.ab",
    "Heob": "Ae.ab", "ae.ab": "Ae.ab", "aeab": "Ae.ab", "AEab": "Ae.ab",
    "Ae.ob": "Ae.ab", "Aelb": "Ae.ab", "Ae1b": "Ae.ab",
    "Aezb": "Ae.ab",  # 'a' misread as 'z'
    "Aedb": "Ae.ab",  # 'a' misread as 'd'
    "Aecb": "Ae.ab",  # 'a' misread as 'c'
    "HAZab": "Ae.ab", "NAeab": "Ae.ab", "AAeab": "Ae.ab",
    "Aab": "Ae.ab", "Alab": "Ae.ab",   # short garbles seen in test output
    "Akab": "Ae.ab", "Aeak": "Ae.ab",
    # Aedes aegypti
    "Ae.ae": "Ae.ae", "aeae": "Ae.ae", "AEae": "Ae.ae",
    "Ae.a3": "Ae.ae", "Ae.oe": "Ae.ae",
    # Culex nigripalpus
    "Cung": "Cx.ng", "cung": "Cx.ng", "Cu.ng": "Cx.ng",
    "Cx.nq": "Cx.ng",  # 'g' misread as 'q'
    "Cxng": "Cx.ng",
    # Psorophora columbiae
    "Psco": "Ps.co", "psco": "Ps.co", "PSco": "Ps.co",
    "Ps.ob": "Ps.ob",
    "Pscb": "Ps.co",   # 'o' misread as 'b'
    "Ps.cb": "Ps.co",
    "Dscb": "Ps.co",   # 'P' misread as 'D' — seen in test output
    # Psorophora ferox
    "Psfx": "Ps.fx", "psfx": "Ps.fx", "Pstx": "Ps.fx", "Psfr": "Ps.fx",
    "Ps.fr": "Ps.fx",  # 'x' misread as 'r'
    "Ps.tx": "Ps.fx",  # 'f' misread as 't'
    # Anopheles
    "Ancr": "An.cr", "Anqu": "An.qu",
    # Ochlerotatus triseriatus
    "Octr": "Oc.tr", "Oc.tr": "Oc.tr",
    "0c.tr": "Oc.tr",  # 'O' misread as '0'
    "Oc.tr.": "Oc.tr",
    # Mansonia
    "Ma.dy": "Ma.dy", "Mady": "Ma.dy",
    "Ma.ti": "Ma.ti", "Mati": "Ma.ti",
    # Coquillettidia
    "Cq.pe": "Cq.pe", "Cqpe": "Cq.pe",
}
