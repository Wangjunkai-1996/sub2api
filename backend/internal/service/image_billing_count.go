package service

// resolveBillableImageCount only bills confirmed outputs. The group's image
// billing switch selects the pricing mode; it cannot invent an image count.
func resolveBillableImageCount(reported int) int {
	return max(reported, 0)
}
