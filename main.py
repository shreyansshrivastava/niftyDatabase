#from compareFiles import compareDataHybrid
import bankNiftyDayCall
import bankNifty15minCall
import bankNifty2hrCall
import pattern_fingerprint
import chart_pattern
from databaseConnection import conn

# ==================Day Call ==================================
bankNiftyDayCall.saveImages()
# ==================15 min Call ==================================
bankNifty15minCall.save_images()
# ==================1hr Call ==================================
bankNifty2hrCall.save_images()
# ==================Pattern FingerPrint Call ==================================
pattern_fingerprint.run()
# ==================Chart pattern Call ==================================
chart_pattern.build_pattern_db()
conn.commit()
conn.close()

