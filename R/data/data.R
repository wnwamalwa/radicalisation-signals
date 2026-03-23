# ================================================================
#  R/data.R — Seed Data, Counties, Shapefile, Case Builder
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

# ── SUB-LOCATION LOOKUP ───────────────────────────────────────────
SUB_LOCATIONS <- list(
  "Nairobi"          = c("Kibera","Kayole","Mathare","Korogocho","Mukuru","Eastleigh","Kasarani","Embakasi","Ruaraka","Dagoretti","Langata","Westlands","Pumwani","Kamukunji","Makadara"),
  "Mombasa"          = c("Likoni","Kisauni","Changamwe","Mvita","Nyali","Jomvu","Miritini","Tudor","Bamburi","Shanzu"),
  "Kisumu"           = c("Kondele","Nyalenda","Manyatta","Bandani","Migosi","Kolwa","Winam","Kisumu East","Seme","Muhoroni"),
  "Nakuru"           = c("Nakuru East","Nakuru West","Naivasha","Gilgil","Molo","Rongai","Njoro","Subukia","Bahati","Kuresoi"),
  "Uasin Gishu"      = c("Langas","Huruma","Pioneer","Kimumu","Moiben","Turbo","Kapsabet","Nandi Hills","Burnt Forest","Eldoret CBD"),
  "Garissa"          = c("Garissa Township","Dadaab","Fafi","Ijara","Lagdera","Balambala","Hulugho"),
  "Mandera"          = c("Mandera East","Mandera West","Mandera North","Banissa","Lafey","Kutulo"),
  "Kakamega"         = c("Kakamega Central","Mumias","Lugari","Malava","Matungu","Butere","Khwisero","Shinyalu","Likuyani"),
  "Meru"             = c("Meru Town","Imenti North","Imenti South","Tigania West","Igembe South","Buuri","Imenti Central"),
  "Nyeri"            = c("Nyeri Town","Tetu","Kieni","Mathira","Othaya","Mukurweini"),
  "Kilifi"           = c("Kilifi North","Kilifi South","Kaloleni","Rabai","Ganze","Malindi","Magarini","Shimo la Tewa"),
  "Kitui"            = c("Kitui Central","Mutomo","Mwingi","Kitui West","Ikutha","Katulani","Nzambani"),
  "Turkana"          = c("Lodwar","Turkana Central","Turkana East","Loima","Turkana South","Kalokol"),
  "Wajir"            = c("Wajir East","Wajir West","Wajir North","Wajir South","Tarbaj","Eldas"),
  "Kisii"            = c("Kisii Central","Bobasi","South Mugirango","Bonchari","Kitutu Chache","Nyaribari Masaba","Masaba North"),
  "Marsabit"         = c("Marsabit Central","Moyale","Laisamis","North Horr","Saku"),
  "Isiolo"           = c("Isiolo North","Isiolo South","Garba Tulla","Merti"),
  "Samburu"          = c("Samburu North","Samburu Central","Samburu East"),
  "Trans Nzoia"      = c("Kiminini","Cherangany","Kwanza","Saboti","Endebess"),
  "West Pokot"       = c("Kapenguria","Sigor","Kacheliba","Pokot South","Pokot North"),
  "Elgeyo-Marakwet"  = c("Marakwet East","Marakwet West","Keiyo North","Keiyo South"),
  "Baringo"          = c("Baringo Central","Baringo North","Baringo South","Mogotio","Eldama Ravine","Tiaty"),
  "Laikipia"         = c("Laikipia West","Laikipia East","Laikipia North","Ol Jorok"),
  "Nyandarua"        = c("Ol Kalou","Kinangop","Kipipiri","Ndaragwa","Mirangine"),
  "Kirinyaga"        = c("Mwea","Gichugu","Ndia","Kirinyaga Central","Mwea East"),
  "Murang'a"         = c("Kangema","Mathioya","Kigumo","Maragwa","Kandara","Gatanga","Kahuro"),
  "Kiambu"           = c("Thika","Ruiru","Githunguri","Gatundu","Juja","Limuru","Kabete","Kikuyu"),
  "Nandi"            = c("Aldai","Nandi Hills","Emgwen","Mosop","Chesumei","Tinderet"),
  "Kericho"          = c("Ainamoi","Belgut","Kipkelion East","Kipkelion West","Soin/Sigowet","Bureti"),
  "Bomet"            = c("Bomet Central","Bomet East","Chepalungu","Konoin","Sotik"),
  "Narok"            = c("Narok North","Narok South","Narok East","Narok West","Emurua Dikirr","Kilgoris"),
  "Kajiado"          = c("Kajiado Central","Kajiado North","Kajiado East","Kajiado West","Loitoktok"),
  "Machakos"         = c("Machakos Town","Mavoko","Masinga","Yatta","Kangundo","Matungulu","Kathiani","Mwala"),
  "Makueni"          = c("Makueni","Kibwezi","Kilome","Kaiti","Mbooni","Nzaui"),
  "Tharaka-Nithi"    = c("Tharaka North","Tharaka South","Chuka","Igambang'ombe","Maara"),
  "Embu"             = c("Manyatta","Runyenjes","Mbeere South","Mbeere North"),
  "Taita Taveta"     = c("Taveta","Wundanyi","Mwatate","Voi"),
  "Kwale"            = c("Msambweni","Lunga Lunga","Matuga","Kinango"),
  "Tana River"       = c("Garsen","Galole","Bura"),
  "Lamu"             = c("Lamu East","Lamu West"),
  "Homa Bay"         = c("Kasipul","Kabondo Kasipul","Karachuonyo","Rangwe","Homa Bay Town","Ndhiwa","Suba North","Suba South"),
  "Migori"           = c("Rongo","Awendo","Suna East","Suna West","Uriri","Nyatike","Kuria West","Kuria East"),
  "Nyamira"          = c("Borabu","Masaba North","Nyamira North","Nyamira South","Kitutu Masaba"),
  "Siaya"            = c("Gem","Rarieda","Alego-Usonga","Ugenya","Ugunja","Bondo"),
  "Busia"            = c("Teso North","Teso South","Nambale","Matayos","Butula","Funyula","Bunyala"),
  "Vihiga"           = c("Luanda","Vihiga","Emuhaya","Hamisi","Sabatia"),
  "Bungoma"          = c("Webuye East","Webuye West","Kimilili","Tongaren","Sirisia","Mt Elgon","Kanduyi","Bumula","Kabuchai")
)

# ── SEED DATA — all 47 counties ──────────────────────────────────
set.seed(42)

counties <- data.frame(
  name  = c(
    "Nairobi","Mombasa","Kisumu","Nakuru","Uasin Gishu",
    "Garissa","Mandera","Kakamega","Meru","Nyeri",
    "Kilifi","Kitui","Turkana","Wajir","Kisii",
    "Marsabit","Isiolo","Samburu","Trans Nzoia","West Pokot",
    "Elgeyo-Marakwet","Baringo","Laikipia","Nyandarua","Kirinyaga",
    "Murang'a","Kiambu","Nandi","Kericho","Bomet",
    "Narok","Kajiado","Machakos","Makueni","Tharaka-Nithi",
    "Embu","Taita Taveta","Kwale","Tana River","Lamu",
    "Homa Bay","Migori","Nyamira","Siaya","Busia",
    "Vihiga","Bungoma"),
  lat   = c(
    -1.286,-4.043,-0.102,-0.303, 0.520,
    -0.454, 3.937, 0.281, 0.047,-0.417,
    -3.629,-1.366, 3.119, 1.747,-0.682,
    2.337, 0.354, 1.061, 1.014, 1.250,
    0.783, 0.617,-0.366,-0.411,-0.659,
    -0.717,-1.031, 0.185,-0.370,-0.798,
    -1.082,-1.852,-1.516,-2.234,-0.301,
    -0.530,-3.383,-4.174, 0.614,-2.273,
    -0.524,-1.063,-0.664, 0.062, 0.461,
    0.062, 0.582),
  lng   = c(
    36.817,39.668,34.762,36.080,35.270,
    39.646,41.855,34.752,37.649,36.950,
    39.852,38.012,35.597,40.058,34.768,
    37.994,37.582,37.072,34.955,35.116,
    35.507,36.026,36.809,36.598,37.288,
    37.047,36.862,35.187,35.286,35.233,
    35.872,36.776,37.257,37.636,37.687,
    37.460,38.358,39.084,40.012,40.912,
    34.459,34.481,34.927,34.289,34.137,
    34.716,34.559),
  count = c(
    187, 78, 95, 62, 54,
    43, 38, 51, 29, 22,
    35, 18, 15, 24, 41,
    12, 10,  8, 19, 11,
    14, 16, 13, 17, 20,
    23, 45, 18, 21, 16,
    25, 28, 32, 14, 11,
    15,  9, 12,  7,  5,
    26, 22, 13, 20, 17,
    12, 24),
  stringsAsFactors=FALSE)

# ── KENYA COUNTIES SF — read directly from shapefile ────────────────
# Place the shapefile folder at:  data/kenyan-counties/County.shp
# (relative to app.R).  All 47 real IEBC county boundaries are used.
.shp_path <- file.path("data", "kenyan-counties", "County.shp")
if (!file.exists(.shp_path))
  stop("Shapefile not found at ", .shp_path,
       ". Place County.shp/.dbf/.shx/.prj in data/kenyan-counties/")

KENYA_SF <- st_read(.shp_path, quiet=TRUE)

# Rename COUNTY → name for consistent joining downstream
names(KENYA_SF)[names(KENYA_SF) == "COUNTY"] <- "name"

# Remap shapefile county names → app county names
KENYA_SF$name[KENYA_SF$name == "Keiyo-Marakwet"] <- "Elgeyo-Marakwet"
KENYA_SF$name[KENYA_SF$name == "Tharaka"]         <- "Tharaka-Nithi"
KENYA_SF$name[KENYA_SF$name == "Murang'A"]        <- "Murang'a"

tmap_mode("view")  # set once at startup, not inside reactive



tweet_texts <- c(
  "Watu wa kabila hiyo hawana haki ya kuishi hapa Kenya yetu",
  "These politicians are corrupt and should face consequences",
  "Kesho tutawaonyesha nguvu yetu, waambie wajiandae!",
  "Let us unite as Kenyans and build this nation together with love",
  "Waende kwao, hawataki amani ya nchi hii",
  "Vote wisely this election, check the record of your candidate",
  "Those people from that region should never be trusted with public office",
  "Najivunia kuwa Mkenya, tunaendelea pamoja",
  "Kabila la wezi hawastahili nafasi katika serikali",
  "Prayer for peace and unity in our beautiful country Kenya",
  "Tukiungana tutashinda, msiyunje ili kupata maendeleo",
  "Heshima kwa wote, hakuna kabila bora kuliko lingine",
  "These migrants should go back to where they came from",
  "Religious extremists are ruining our peaceful coexistence",
  "Sisi sote ni ndugu, tunaomba amani na upendo Kenya nzima"
)

target_groups <- c(
  "Kikuyu community","Coast residents","Luo community","Kalenjin voters",
  "Somali residents","Northern Kenya pastoralists","Western Kenya farmers",
  "Opposition leaders","Government officials","Religious minorities",
  "Migrant workers","Women politicians","Youth groups","Business community","Civil society"
)

platforms  <- c("Twitter/X","Facebook","TikTok","Telegram","WhatsApp")
languages  <- c("Swahili","English","Sheng","Kikuyu","Luo","Kalenjin")
categories <- c("Ethnicity","Religion","Gender","Political","Regional","Incitement")
handles    <- c("@user_ke","@nairobi_talk","@kisumu_voice","@mombasa_news","@kenyatalk")

seed_ncic_levels  <- c("5","4","0","0","2","3","4","5")
seed_officers     <- c("Officer Mwangi","Officer Njeri","Officer Otieno",NA,NA,NA)

signal_presets <- c(
  "Extremist keyword|Us-vs-them framing|Network exposure",
  "Ethnic slur detected|Incitement language|Historical pattern match",
  "No hate signals detected",
  "Religious intolerance marker|Political targeting",
  "Dehumanising language|Coordinated posting|PEV-era pattern"
)

build_cases <- function() {
  do.call(rbind, lapply(seq_len(nrow(counties)), function(i) {
    n          <- counties$count[i]
    idx        <- seq(sum(counties$count[seq_len(i-1)])+1,
                      sum(counties$count[seq_len(i)]))
    target_idx <- sample(seq_len(nrow(counties))[-i], n, replace=TRUE)
    tweets     <- sample(tweet_texts, n, replace=TRUE)
    ncic_lvls  <- sample(seed_ncic_levels, n, replace=TRUE)
    confs      <- sample(62:99, n, replace=TRUE)
    net_sc     <- sample(10:50, n, replace=TRUE)
    freq_sp    <- sample(5:30,  n, replace=TRUE)
    secs_ago   <- sample(0:(30*86400), n, replace=TRUE)
    validated  <- sample(seed_officers, n, replace=TRUE)
    val_ts     <- ifelse(!is.na(validated),
                         format(Sys.time() - secs_ago/2, "%Y-%m-%d %H:%M"), NA_character_)
    
    risks <- mapply(compute_risk,
                    tweet=tweets, gpt_conf=confs, ncic_level=ncic_lvls,
                    MoreArgs=list(kw_weights=kw_weights_global,
                                  network_score=20, freq_spike=10, ctx_score=0),
                    SIMPLIFY=FALSE)
    
    data.frame(
      case_id          = paste0("KE-", formatC(idx, width=5, flag="0")),
      county           = counties$name[i],
      sub_location     = sample(SUB_LOCATIONS[[counties$name[i]]] %||%
                                  c("Central","East","West","North","South"), n, replace=TRUE),
      src_lat          = counties$lat[i],
      src_lng          = counties$lng[i],
      target_county    = counties$name[target_idx],
      tgt_lat          = counties$lat[target_idx],
      tgt_lng          = counties$lng[target_idx],
      target_group     = sample(target_groups, n, replace=TRUE),
      platform         = sample(platforms, n, replace=TRUE),
      handle           = sample(handles, n, replace=TRUE),
      tweet_text       = tweets,
      language         = sample(languages, n, replace=TRUE),
      ncic_level       = as.integer(ncic_lvls),
      ncic_label       = sapply(ncic_lvls, ncic_name),
      section_13       = sapply(ncic_lvls, ncic_s13),
      confidence       = paste0(confs,"%"),
      conf_num         = confs,
      conf_band        = sapply(confs, conf_band),
      category         = sample(categories, n, replace=TRUE),
      validated_by     = validated,
      validated_at     = val_ts,
      action_taken     = ifelse(!is.na(validated),
                                sample(c("CONFIRMED","DOWNGRADED","CLEARED"), n, replace=TRUE),
                                NA_character_),
      officer_ncic_override = NA_integer_,
      risk_score       = sapply(risks, `[[`, "score"),
      risk_formula     = sapply(risks, `[[`, "formula"),
      kw_score         = sapply(risks, `[[`, "kw_score"),
      network_score    = net_sc,
      signals          = sample(signal_presets, n, replace=TRUE),
      trend_data       = sapply(seq_len(n), function(j) {
        base <- sample(10:80,1)
        paste(pmin(100,pmax(0, base+cumsum(sample(-8:10,7,replace=TRUE)))),
              collapse=",")
      }),
      timestamp        = as.POSIXct(Sys.time()-secs_ago,
                                    origin="1970-01-01", tz="Africa/Nairobi"),
      stringsAsFactors = FALSE
    )
  }))
}


# ── LOAD OR SEED CASES FROM DB ───────────────────────────────────
# On first run: DB is empty → seed with generated cases and persist.
# On subsequent runs: load directly from DB — all validations intact.
{
  db_cases <- db_load_cases()
  if (is.null(db_cases) || nrow(db_cases) == 0) {
    message("[db] No cases found — seeding with generated data...")
    all_cases <- build_cases()
    all_cases$risk_level    <- with(all_cases,
                                    ifelse(risk_score>=65,"HIGH",
                                           ifelse(risk_score>=35,"MEDIUM","LOW")))
    all_cases$timestamp_chr <- format(all_cases$timestamp, "%Y-%m-%d %H:%M")
    all_cases$notes         <- ""
    db_save_cases(all_cases)
    message(sprintf("[db] Seeded %d cases", nrow(all_cases)))
  } else {
    all_cases <- db_cases
    message(sprintf("[db] Loaded %d cases from DB", nrow(all_cases)))
  }
}

DATE_MIN <- as.Date(min(all_cases$timestamp))
DATE_MAX <- as.Date(max(all_cases$timestamp))

