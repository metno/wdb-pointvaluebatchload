SOURCE = \
	src/AbstractDatabaseJob.cpp \
	src/AbstractDatabaseJob.h \
	src/AbstractJob.cpp \
	src/AbstractJob.h \
	src/Configuration.cpp \
	src/Configuration.h \
	src/DataQueue.cpp \
	src/DataQueue.h \
	src/InputData.cpp \
	src/InputData.h \
	src/TranslateJob.cpp \
	src/TranslateJob.h \
	src/OldStyleTranslateJob.cpp \
	src/OldStyleTranslateJob.h \
	src/NewStyleTranslateJob.cpp \
	src/NewStyleTranslateJob.h \
	src/DatabaseTranslator.cpp \
	src/DatabaseTranslator.h \
	src/CopyJob.cpp \
	src/CopyJob.h \
	src/FloatValueGroup.cpp \
	src/FloatValueGroup.h \
	src/timetypes.cpp \
	src/timetypes.h
	
wdb_fastload_SOURCES += \
	src/main.cpp
	
libfastload_a_SOURCES += $(SOURCE)
