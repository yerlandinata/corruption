import re
import csv
from Levenshtein import distance
from collections import Counter
import csv
import datetime
import hashlib

PARENTHESES = r'\(.*\)'
ALIASES = r'alias.*'
ACADEMIC_TITLE = r',\s{0,2}[A-Z]\.[A-Z]+[a-z]*'
ACADEMIC_TITLE_NOTFORMAL = r',\s{0,2}[A-Z]{2}'
HONORIFIC_TITLE = r'[A-Z][a-z]{0,3}\.'

PROVINCES = set()
CITIES = dict()

def load_provinces_and_cities(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            province, city = line.strip().split(',')
            PROVINCES.add(province.strip())
            CITIES[city.strip()] = province
    
PROVINCES = {*PROVINCES, 'DKI', 'DIY', 'NTB', 'NTT'}

def save_corruption_data(work_dir, corruptions):
  with open(work_dir + '/korupsi_result' + str(int(datetime.datetime.now().timestamp())) + '.csv', 'w') as f:
    f.write('id,jenis perkara,tahun,koruptor,organisasi terlibat,kota/kabupaten,provinsi,hukuman vonis,tanggal vonis,tanggal ott,kerugian negara,web search,keterangan,tahun kpk\n')
    for c in corruptions:
      f.write(str(c) + '\n')

class CorruptionCategory:
  def __init__(self, name, confidence, evidences, search_query=None):
    self.name = name
    if search_query is None:
      self.search_query = self.name
    else:
      self.search_query = search_query
    self.confidence = confidence
    self.evidences = {*evidences, name}
    
  def match(self, description):
    description = description.lower()
    for evidence in self.evidences:
      if evidence in description:
        return True
    return False
  
  def __eq__(self, o):
    if type(o) != CorruptionCategory:
      return False
    return self.name == o.name
  
  def __repr__(self):
    return self.__str__()
  
  def __str__(self):
    return self.name
  
corruption_categories = sorted([
  CorruptionCategory('pengadaan', 205 - 17, ['pembangunan', 'proyek', 'fiktif', 'pekerjaan']),
  CorruptionCategory('izin', 23 - 0, ['ijin', 'surat'], search_query='perizinan'),
  CorruptionCategory('suap', 661 - 97, ['penyuapan', 'hadiah', 'janji', 'nerima', 'gratifikasi']),
  CorruptionCategory('pungli', 26 - 1, ['pungut', 'pemungutan', 'meminta', 'pembayaran']),
  CorruptionCategory('anggaran', 48 - 2, ['guna']),
  CorruptionCategory('tppu', 34 - 3, ['cuci', 'laundry'], search_query='pencucian uang'),
  CorruptionCategory('rintangi', 10 - 0, ['ringtangi'], search_query='merintangi kpk')
], key=lambda c: -c.confidence)

CENTRAL_GOVERNMENTS = [
    'kementerian',
    'kementrian',
    'dpr ri',
    'dpd ri',
    'mahkamah',
    'kpu',
    'bpk',
    'bpkp',
    'ma',
    'mk',
    'komisi yudisial',
    'kejaksaan agung',
    'ojk',
    'bank indonesia',
    'otoritas jasa keuangan',
    'bumn',
]


empty_if_none = lambda s: '' if s is None else s
none_if_empty = lambda s: s if s is not None else None

class Corruption:
  
  @staticmethod
  def get_batch_from_csv(file_handle, corruption_categories):
    corruption_categories = {c.name: c for c in corruption_categories}
    csv_reader = csv.reader(file_handle, delimiter=',', quotechar='"')
    corruptions = []
    for i, row in enumerate(csv_reader):
      if i == 0:
        continue # header row
#       print(row)
      key, category, year, accused, organization, city, province, google_search, desc, src = row
      key = int(key)
      category = corruption_categories[category] if category != '' else None
      year = none_if_empty(year)
      accused = none_if_empty(accused)
      organization = none_if_empty(organization)
      city = none_if_empty(city)
      province = none_if_empty(province)
      google_search = eval(google_search) if google_search != '' else None
      corruptions.append(Corruption(key, desc, accused, organization, src, city=city, province=province, google_search=google_search, year=year, category=category))
    return corruptions
    
  @staticmethod
  def get_batch_from_csv_v2(file_handle, corruption_categories):
    corruption_categories = {c.name: c for c in corruption_categories}
    csv_reader = csv.reader(file_handle, delimiter=',', quotechar='"')
    corruptions = []
    for i, row in enumerate(csv_reader):
      if i == 0:
        continue # header row
#       print(row)
      key, category, year, accused, organization, city, province, verdict_punishment, verdict_date, arrest_date, state_damage, google_search, desc, src = row
      key = int(key)
      category = corruption_categories[category] if category != '' else None
      year = none_if_empty(year)
      accused = none_if_empty(accused)
      organization = none_if_empty(organization)
      city = none_if_empty(city)
      province = none_if_empty(province)
      verdict_date = none_if_empty(verdict_date)
      verdict_punishment = none_if_empty(verdict_punishment)
      arrest_date = none_if_empty(arrest_date)
      state_damage = none_if_empty(arrest_date)
      google_search = eval(google_search) if google_search != '' else None
      corruptions.append(
        Corruption(
          key, desc, accused, organization, src, city=city, province=province, 
          verdict_date=verdict_date, arrest_date=arrest_date, verdict_punishment=verdict_punishment, state_damage=state_damage,
          google_search=google_search, year=year, category=category
          )
        )
    return corruptions

  def __init__(self, key, desc, accused, organization, src, city=None, province=None, google_search=None, year=None, category=None, verdict_date=None, arrest_date=None, verdict_punishment=None, state_damage=None):
    self.original_data = desc + ' ' + accused + ' ' + organization
    self.key = key
    self.src = src
    self.desc = desc.replace('\n', ' ').strip()
    self.accused = accused.replace('\n', '').strip()
    self.organization = organization.replace('\n', '').strip()
    self.year = year
    self.google_search = google_search
    self.category = category
    self.city = city
    self.province = province
    self.verdict_date = verdict_date
    self.arrest_date = arrest_date
    self.verdict_punishment = verdict_punishment
    self.state_damage = state_damage
    self.fill_year()
    self.clean_accused()
    self.fill_location()
    
  def fill_location(self):
    lower_full_desc = self.original_data.lower()
    if self.city is None:
      city_counter = Counter()
      for city in CITIES.keys():
        city_counter[city] = len(re.findall('(?:\s|^)'+city.lower()+'(\s|$)', lower_full_desc))
      if city_counter.most_common(1)[0][1] > 0:
        self.city = city_counter.most_common(1)[0][0]
    if self.province is None:
      prov_counter = Counter()
      for prov in PROVINCES:
        prov_counter[prov] = len(re.findall('(?:\s|^)'+prov.lower()+'(\s|$)', lower_full_desc))
      if prov_counter.most_common(1)[0][1] > 0:
        self.province = prov_counter.most_common(1)[0][0]
    if self.province is None and self.city is not None:
      self.province = CITIES[self.city]
    if self.province is None and self.city is None:
      for govt_agency in CENTRAL_GOVERNMENTS:
        if len(re.findall('(?:\s|^)'+govt_agency.lower()+'(\s|$)', lower_full_desc)) > 0:
          self.province = 'Jakarta'
          return
    
  def fill_category(self, sorted_category):
    if self.category is None:
      for c in sorted_category:
        if c.match(self.desc):
          self.category = c
          return
    
  def fill_year(self):
    if self.year is None:
      for token in self.desc.split():
        if len(token) == 4 and token[:2] == '20':
          try:
            int(token)
            self.year = token
            return
          except:
            pass
        
  def clean_accused(self):
    cleaned = re.sub(PARENTHESES, '', self.accused)
    cleaned = re.sub(ALIASES, '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(ACADEMIC_TITLE, '', cleaned)
    cleaned = re.sub(ACADEMIC_TITLE_NOTFORMAL, '', cleaned)
    cleaned = re.sub(HONORIFIC_TITLE, '', cleaned)
    self.accused = cleaned.strip().replace('.', '')
    
  def __str__(self):
    return '"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}"'.format(
        self.key, empty_if_none(self.category), empty_if_none(self.year), self.accused, 
        self.organization, empty_if_none(self.city), empty_if_none(self.province), 
        empty_if_none(self.verdict_punishment), empty_if_none(self.verdict_date), empty_if_none(self.arrest_date), empty_if_none(self.state_damage),
        empty_if_none(self.google_search), self.desc, self.src
    )
  
  def __repr__(self):
    return self.__str__()
  
  def __eq__(self, o):
    if type(o) != Corruption:
      return False
    
    # considered same case if the accused is the same
    min_dist = 1
    if len(self.accused) > 5:
      min_dist = 2
    elif len(self.accused) > 12:
      min_dist = 3
    if distance(self.accused.lower(), o.accused.lower()) <= min_dist and self.category == o.category:
      return True
    return False
    
  def get_search_query(self):
    return (
      'vonis korupsi {} {} {} {}'.format(empty_if_none(self.category), self.accused, self.organization, empty_if_none(self.year)),
      'operasi tangkap tangan ott korupsi {} {} {} {}'.format(empty_if_none(self.category), self.accused, self.organization, empty_if_none(self.year)),
      'kerugian negara korupsi {} {} {} {}'.format(empty_if_none(self.category), self.accused, self.organization, empty_if_none(self.year)),
    )

def generate_webpage_id(case, url):
  accused_first_name = case.accused.split()[0].lower()
  url_hash = hashlib.md5(url.encode()).hexdigest()
  return '{}_{}'.format(accused_first_name, url_hash)
