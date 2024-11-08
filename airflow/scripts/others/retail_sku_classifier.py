def cts(x, y):
    if y.lower() in x.lower():
        return True
    else:
        return False
    
def classify_sku(x):

    if cts(x, "табак skala"): return "ЖТ SKALA"
    elif cts(x, 'split s'): return "SPLIT S"
    elif cts(x, "MINICAN PLUS SLIDER"): return "MINICAN SLIDER"
    elif cts(x, "BRUSKO MINICAN PRO PLUS"): return "MINICAN PRO PLUS"

    elif cts(x, 'split l'): return "SPLIT L"
    elif cts(x, "VINI"): return "VINI"
    elif cts(x, "LONGPARTY 5000"): return "BRUSKO LONGPARTY 5000"
    elif cts(x, "LONGPARTY 6000"): return "BRUSKO LONGPARTY 6000"
    elif cts(x, "MAGIC 3000"): return "BRUSKO MAGIC 3000"
    elif cts(x, "LONGPARTY 9000"): return "BRUSKO LONGPARTY 9000"
    elif cts(x, "BRUSKO NRG"): return "BRUSKO NRG"
    elif cts(x, "DABBLER 1500"): return "DABBLER 1500"
    elif cts(x, "DABBLER 4000"): return "DABBLER 4000"
    elif cts(x, "DABBLER 5000"): return "DABBLER 5000"
    elif cts(x, "DABBLER 6000"): return "DABBLER 6000"
    elif cts(x, "DABBLER TURBO"): return "DABBLER TURBO"
    elif cts(x, "VAPOR SPACE"): return "MONSTERVAPOR SPACE"
    elif cts(x, "SKALA ICE"): return "SKALA ICE"
    elif cts(x, "VAPE STORM"): return "ANGRY VAPE STORM"
    elif cts(x, "RAGE STICK"): return "ANGRY VAPE RAGE STICK"
    elif cts(x, "MEW 4"): return "MEW 4000"
    # DABBLER
    elif cts(x, "ОСДН DABBLER"): return "ОСДН DABBLER"
    elif cts(x, 'dabbler') and cts(x, 'ultra'): return "Dabbler 2 Ultra"
    elif cts(x, 'dabbler salt') and cts(x, 'chub'): return "Dabbler Salt Chubby" 
    elif cts(x, 'dabbler salt') and not(cts(x, 'chub')): return "Dabbler Salt (стар.)" 
    elif cts(x, 'dabbler nice plus') and not(cts(x, 'картридж')): return "Dabbler Nice Plus"
    elif cts(x, 'dabbler nice') and not(cts(x, 'картридж')): return "Dabbler Nice"
    elif cts(x, 'dabbler nice') and (cts(x, 'картридж')): return "Картридж Dabbler Nice"
    # Brusko Go
    elif cts(x, "Brusko go max"): return "Go Max"
    elif cts(x, "Brusko go mega"): return "Go Mega"
    elif cts(x, "Brusko go giga"): return "Go Giga"
    elif cts(x, "Brusko Go"): return "Go"
    # APX S1
    elif cts(x, "APX S1") and cts(x, "Картридж"): return  "Картридж APX S1"
    elif cts(x, "APX S1") and not(cts(x, "Картридж")): return  "APX S1"
    # APX C1
    elif cts(x, "APX C1") and cts(x, "Картридж"): return  "Картридж APX C1"
    elif cts(x, "APX C1") and not(cts(x, "Картридж")): return  "APX C1"
    # Cloudflask
    elif cts(x, "CLOUDFLASK") and cts(x, "Испаритель"): return  "Испаритель CLOUDFLASK"
    elif cts(x, "CLOUDFLASK") and cts(x, "Картридж"): return  "Картридж CLOUDFLASK"
    elif cts(x, "CLOUDFLASK") and not(cts(x, "Испаритель")): return  "CLOUDFLASK"

    # PAGEE AIR
    elif cts(x, "PAGEE AIR"): return "PAGEE AIR"

    # Flexus
    elif cts(x, "Flexus Q") and cts(x, "Картридж"): return "Картридж Flexus Q"
    elif cts(x, "Flexus Q") and not(cts(x, "Картридж")): return "Flexus Q"

    elif cts(x, "AF Mesh Coil") and not(cts(x, "MINICAN")): return "Испаритель Flexus"

    elif cts(x, "Flexus Stik") and not(cts(x, "Картридж")): return "Flexus Stik"
    elif cts(x, "Flexus Stik") and (cts(x, "Картридж")): return "Картридж Flexus Stik"

    elif cts(x, "Flexus Fit") and not(cts(x, "Картридж")): return "Flexus Fit"
    elif cts(x, "Flexus Fit") and (cts(x, "Картридж")): return "Картридж Flexus Fit"

    elif cts(x, "Flexus Blok") and not(cts(x, "Картридж")): return "Flexus Blok"
    elif cts(x, "Flexus Blok") and (cts(x, "Картридж")): return "Картридж Flexus Blok"

    # RIIL X
    elif cts(x, "RIIL") and not(cts(x, "Картридж")): return "RIIL X"
    elif cts(x, "RIIL") and (cts(x, "Картридж")): return "Картридж RIIL X"

    # AV FURY
    elif cts(x, "Vape Fury") and not(cts(x, "Картридж")): return "Angry Vape Fury"
    elif cts(x, "VapeFury") and (cts(x, "Картридж")): return "Картридж Angry Vape Fury"
    
    # HAZE
    elif cts(x, "HAZE") and (cts(x, "Кальян")): return "Кальян HAZE"
    #FAVOSTIX
    elif cts(x, "Favostix Mini") and not(cts(x, "Картридж")): return "FAVOSTIX MINI"

    elif cts(x, "Favostix") and not(cts(x, "Картридж")): return "FAVOSTIX"
    elif cts(x, "Favostix") and cts(x, "Картридж"): return "Картридж FAVOSTIX"

    #FEELIN MINI
    elif cts(x, "Feelin mini") and (not(cts(x, "Испаритель")) and not(cts(x, "Картридж"))): return "FEELIN MINI"
    elif cts(x, "Feelin") and (cts(x, "Испаритель")): return "Испаритель FEELIN"
    elif cts(x, "Feelin") and (cts(x, "Картридж")): return "Картридж FEELIN MINI"
    elif cts(x, "Feelin X") and not(cts(x, "Mini")): return "Feelin X"
    elif cts(x, "Feelin") and not(cts(x, "Mini")): return "Feelin"

    elif cts(x, "Minican") and cts(x, "Испаритель"): return "Испаритель MINICAN"
    elif cts(x, "Minican 5") and cts(x, "Картридж"): return "Картридж MINICAN 5"
    elif cts(x, "Minican 4") and cts(x, "Картридж"): return "Картридж MINICAN 4"
    elif cts(x, "Minican 3") and cts(x, "Картридж"): return "Картридж MINICAN 3"
    elif cts(x, "Prefilled"): return "PREFILLED PODS"

    elif cts(x, "MINICAN") and (cts(x, "Картридж")): return "Картридж MINICAN"
    elif cts(x, "MINICAN 3") and (cts(x, "Картридж")): return "Картридж MINICAN 3"
    elif cts(x, "MINICAN") and (cts(x, "Испаритель")): return "Испаритель MINICAN"    
    elif cts(x, "Minican Plus"): return "MINICAN PLUS"
    elif cts(x, "Minican 5 Pro"): return "MINICAN 5 PRO"
    elif cts(x, "Minican 5"): return "MINICAN 5"
    elif cts(x, "Minican 4"): return "MINICAN 4"
    elif cts(x, "Minican 2"): return "MINICAN 2"
    elif cts(x, "Minican 3"): return "MINICAN 3"
    elif cts(x, "Disposable"): return "MINICAN DISPOSABLE"
    elif cts(x, "Одноразовая") and cts(x, "Minican"): return  "MINICAN DISPOSABLE"
    elif cts(x, "Minican"): return "MINICAN"

    # MICOOL
    elif cts(x, "Чехол"): return "Чехол MICOOL"
    
    elif cts(x, "MICOOL") and not(cts(x, "Картридж")): return "MICOOL"
    elif cts(x, "MICOOL") and (cts(x, "Картридж")): return "Картридж MICOOL"

    # VILTER
    elif cts(x, "Фильтр"): return "Мундштук VILTER"    
    elif cts(x, "Vilter Power Bank"): return "VILTER PB"   
    elif cts(x, "Vilter PB"): return "VILTER PB"    
    elif cts(x, "Vilter Fun"): return "VILTER FUN"    
    elif cts(x, "Vilter S"): return "VILTER S"

    elif cts(x, "VILTER PRO") and not(cts(x, "Картридж")): return "VILTER PRO"
    elif cts(x, "VILTER PRO") and (cts(x, "Картридж")): return "Картридж VILTER PRO" 
    
    
    elif cts(x, "Vilter") and not(cts(x, "Картридж")): return "VILTER"
    elif cts(x, "Vilter") and (cts(x, "Картридж")): return "Картридж VILTER"

    # PIXEL PRO
    elif cts(x, "Pixel pro"): return "PIXEL PRO"
    elif cts(x, "0,33 л"): return "Лимонад БРУСКО 0,33 л"
    elif cts(x, "30 л"): return "Лимонад БРУСКО 30 л"
    elif cts(x, "соус "): return "Соусы для кальяна"   

    # Стартер паки
    elif cts(x, "STARTER PACK"): return "BRUSKO STARTER PACK"
    # БКС
    # 50 Медиум
    elif cts(x, "смесь для кальяна BRUSKO") and (cts(x, " 50") and cts(x, "Medium")): return "BRUSKO MEDIUM 50 г"
    # 50 Зеро
    elif cts(x, "смесь для кальяна BRUSKO") and (cts(x, " 50") and cts(x, "Zero")): return "BRUSKO ZERO 50 г"    
    # 50 Стронг    
    elif cts(x, "месь для кальяна BRUSKO") and (cts(x, " 50") and cts(x, "Strong")): return "BRUSKO STRONG 50 г" 
    # 250 Медиум
    elif cts(x, "месь для кальяна BRUSKO") and (cts(x, "250") and cts(x, "Medium")): return "BRUSKO MEDIUM 250 г"
    # 250 Зеро
    elif cts(x, "месь для кальяна BRUSKO") and (cts(x, "250") and cts(x, "Zero")): return "BRUSKO ZERO 250 г"
    # 250 Стронг    
    elif cts(x, "месь для кальяна BRUSKO") and (cts(x, "250") and cts(x, "Strong")): return "BRUSKO STRONG 250 г"
    # BIT
    elif cts(x, "BRUSKO BIT"): return "BRUSKO BIT"

    # Табак
    elif (cts(x, "Табак для кальяна BRUSKO") and cts(x, " 25")): return "Табак BRUSKO 25 г"
    elif (cts(x, "Табак для кальяна BRUSKO") and cts(x, " 125")): return "Табак BRUSKO 125 г"
    elif (cts(x, "Табак для кальяна BRUSKO") and cts(x, " 250")): return "Табак BRUSKO 250 г"

    # Снюс
    elif cts(x, "FAVE"): return "ЖТ BRUSKO FAVE"
    elif cts(x, "ANGRY CHEW"): return "ЖТ ANGRY CHEW"
    elif cts(x, "HAPPMAN"): return "ЖТ HAPPMAN"
    elif cts(x, "Monster Chewer"): return "ЖТ MONSTER CHEWER"
    elif cts(x, "SKALA") and cts(x, "жеват"): return "ЖТ SKALA"
    # Уголь
    elif cts(x, "Уголь"): return "Уголь"
    elif cts(x, "кокосовый BRUSKO") and cts(x, "12"): return "Уголь BRUSKO 12 шт"
    elif cts(x, "кокосовый BRUSKO") and cts(x, "72"): return "Уголь BRUSKO 72 шт"
    elif cts(x, "Кокосовый уголь для кальяна 25") and cts(x, "12"): return "Уголь BRUSKO 12 шт"
    elif cts(x, "Кокосовый уголь для кальяна 25") and cts(x, "72"): return "Уголь BRUSKO 72 шт"
    elif cts(x, "Кокосовый уголь для кальяна 25"): return "Уголь BRUSKO"
    # Жидкости
    elif cts(x, "Brusko, 30 мл"): return "BRUSKO 30 мл (щелочь)"
    elif cts(x, "Brusko, 60 мл"): return "BRUSKO 60 мл"
    elif cts(x, "Brusko Salt, 10 мл"): return "BRUSKO SALT 10 мл"
    elif cts(x, "Brusko Salt, 30 мл") and cts(x, "2 ultra"): return "BRUSKO SALT 2 Ultra"
    elif cts(x, "Brusko Salt, 30 мл") and not(cts(x, "2 ultra")): return "BRUSKO SALT 2"
    elif cts(x, "Жидкость MONSTERVAPOR, 30") and not(cts(x, ", 0")): return "MONSTERVAPOR 30 мл"
    elif cts(x, "Жидкость MONSTERVAPOR") and cts(x, ", 0"): return "MONSTERVAPOR ZERO"
    elif cts(x, "Жидкость MONSTERVAPOR Salt") and cts(x, ", 10"): return "MONSTERVAPOR SALT 10 мл"
    elif cts(x, "Жидкость ANGRY VAPE") and cts(x, ", 10"): return "ANGRY VAPE 10 мл"
    elif cts(x, "Жидкость ANGRY VAPE") and cts(x, ", 0"): return "ANGRY VAPE ZERO"
    elif cts(x, "ANGRY VAPE SALT X PODONKI"): return "ANGRY VAPE PODONKI"
    elif cts(x, "Жидкость ANGRY VAPE, 10") and cts(x, ", 0"): return "ANGRY VAPE 10 мл"
    elif cts(x, "Жидкость SKALA Salt, 30") and cts(x, "2 ultra"): return "SKALA 2 Ultra"
    elif cts(x, "Жидкость SKALA Salt, 30") and not(cts(x, "2 ultra")): return "SKALA 2"
    elif cts(x, "Жидкость SKALA") and cts(x, ", 0"): return "SKALA ZERO"
    elif cts(x, "Жидкость SKALA Salt, 10") and not(cts(x, ", 0")): return "SKALA 10 мл"
    elif cts(x, "Жидкость Dabbler") and cts(x, ", 0"): return "Dabbler ZERO"
    elif cts(x, "Жидкость Dabbler, 10") and cts(x, ", 0"): return "Dabbler 10 мл"




    else:
        return 'Прочее'
        
def classify_macro(x):

    if cts(x, "Жидкость"): return "Жидкость"
    elif cts(x, "Жевательный"): return "Жевательный табак"
    elif cts(x, "Одноразовая") or cts(x, "ОСДН"): return "Одноразовые ЭС"
    elif cts(x, "Многоразовая"): return "Электронные сигареты"
    elif cts(x, "Напиток"): return "Лимонад"
    elif cts(x, "Вапорайзер"): return "Электронные сигареты"
    elif cts(x, "ЭС") and not(cts(x, "Одноразовая")): return "Электронные сигареты"
    elif cts(x, "Испаритель") or cts(x, "Картридж"): return "Расходники"
    elif cts(x, "табак для") or cts(x, "смесь для"): return "Кальянные смеси"
    else: return "Прочее"