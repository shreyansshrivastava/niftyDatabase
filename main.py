#from compareFiles import compareDataHybrid
from niftyCall import  loadDataFromFile1Hr
# from niftyCall import bankNifty2hrCall, bankNiftyDayCall, bankNifty15minCall, dailyData, Nifty50DayCall, \
#     loadDataFromFile
# from niftyPattern import pattern_fingerprint, chart_pattern
from databaseFile.databaseConnection import conn

# ==================Day BnkNifty Call ==================================
# bankNiftyDayCall.saveImages()
# print("Completed bankNiftyDayCall.saveImages() ")
# # ==================15 min Call ==================================
# bankNifty15minCall.save_images()
# print("Completed bankNifty15minCall.save_images() ")
# # ==================1hr Call ==================================
# bankNifty2hrCall.save_images()
# print("Completed bankNifty2hrCall.save_images() ")
# # ==================Pattern FingerPrint Call ==================================
# pattern_fingerprint.run()
# print("Completed pattern_fingerprint.run() ")
# # ==================Chart pattern Call ==================================
# chart_pattern.build_pattern_db()
# print("Completed chart_pattern.build_pattern_db() ")
# #==============================Day price ====================================
# # dailyData.saveImages()
# print("Completed dailyData.saveImages()) ")
# ==================Day Nifty 50 Call ==================================
# Nifty50DayCall.saveImages()
# print("Completed bankNiftyDayCall.saveImages() ")

#loadDataFromFile.process_and_upload_nifty("C:\\Users\\shrey\\Downloads\\NIFTY BANK - 15 minute_with_indicators_.csv")
loadDataFromFile1Hr.process_and_upload_nifty("C:\\Users\\shrey\\Downloads\\NIFTY BANK - 15 minute_with_indicators_.csv")

conn.commit()

conn.close()

