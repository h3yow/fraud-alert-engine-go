package engine

import "strconv"

func formatAmount(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
