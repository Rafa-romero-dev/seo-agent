# List of common User-Agents to rotate (Prevents 403 Forbidden errors)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/605.1.15",
]

# Domains to filter out
DIRECTORY_DOMAINS = [
    # --- Job Boards ---
    'indeed', 'glassdoor', 'ziprecruiter', 'simplyhired', 'linkedin', 
    'snagajob', 'monster.com', 'careerbuilder', 'jooble', 'talent.com', 
    'upwork', 'fiverr', 'salary.com', 'lensa', 'postjobfree',

    # --- Government & Official ---
    '.gov', 'texas.gov', 'dot.state', 'fmcsa', 'osha.gov', 'usps.com', 
    'police', 'sheriff', 'cityof', 'countyof',

    # --- Directories & Aggregators ---
    'yelp', 'yellowpages', 'bbb.org', 'mapquest', 'angieslist', 
    'thumbtack', 'nextdoor', 'porch', 'cargurus', 'mechanicadvisor', 
    'repairpal', '4roadservice', 'findtruckservice', 'truckdown', 
    'nttrdirectory', 'truckerguideapp', 'chamberofcommerce', 
    'carfax', 'kbb.com', 'edmunds', 'autotrader', 'superpages', 
    'dexknows', 'whitepages', 'trustpilot', 'groupon', 'local.yahoo',

    # --- Social Media & Big Tech ---
    'facebook', 'instagram', 'tiktok', 'youtube', 'twitter', 'pinterest', 
    'wikipedia', 'reddit', 'medium', 'google', 'apple', 'amazon',

    # --- National Fleet / Rental / Corporate ---
    'uhaul', 'penske', 'budgettruck', 'ryder', 'enterprise', 
    'loves.com', 'pilotflyingj', 'ta-petro', 'flyj', 'hertz', 
    'goodyear', 'firestone', 'pepboys', 'autozone', 'oreillyauto', 
    'advanceautoparts', 'napaonline', 'discounttire', 'michelinman', 
    'safelite', 'aamco', 'meineke', 'jiffylube', 'valvoline', 
    'ford.com', 'chevrolet.com', 'toyota.com', 'honda.com', 'dodge.com'
]

# Filter out "emails" that are actually file names or system addresses
JUNK_EMAIL_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.js', '.css', '.woff', '.ttf']
JUNK_EMAIL_PREFIXES = ['sentry', 'noreply', 'no-reply', 'hostmaster', 'postmaster', 'webmaster', 'example']
JUNK_EMAIL_DOMAINS = ['wix.com', 'godaddy.com', 'squarespace.com', 'sentry.io', 'wordpress.com', 'google.com', 'yandex.ru', 'example.com']
