#from compareFiles import compareDataHybrid
from niftyCall import bankNifty2hrCall, bankNiftyDayCall, bankNifty15minCall, dailyData
from niftyPattern import pattern_fingerprint, chart_pattern
from databaseFile.databaseConnection import conn

# ==================Day Call ==================================
# bankNiftyDayCall.saveImages()
# ==================15 min Call ==================================
# bankNifty15minCall.save_images()
# ==================1hr Call ==================================
# bankNifty2hrCall.save_images()
# ==================Pattern FingerPrint Call ==================================
# pattern_fingerprint.run()
# ==================Chart pattern Call ==================================
# chart_pattern.build_pattern_db()
#==============================Day price ====================================
dailyData.saveImages()


conn.commit()
conn.close()

