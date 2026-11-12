package main

// slotOf 与 Redis 7.0.15 的 keyHashSlot 一致：CRC-16/CCITT-FALSE，再对 16384 取低位。
// hash tag 只认第一对花括号，括号里至少要有一个字符。
func slotOf(key string) int {
	s := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			s = i
			break
		}
	}
	if s >= 0 {
		for e := s + 1; e < len(key); e++ {
			if key[e] == '}' {
				if e > s+1 {
					key = key[s+1 : e]
				}
				break
			}
		}
	}
	return int(crc16([]byte(key)) & 0x3FFF)
}

func crc16(buf []byte) uint16 {
	var crc uint16
	for _, b := range buf {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
