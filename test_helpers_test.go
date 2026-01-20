package roaringsearch

import (
	"math/rand"
	"strings"
)

// Word pools for generating realistic documents
var (
	commonWords = []string{
		"the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
		"it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
		"this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
		"or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
		"about", "after", "again", "against", "age", "also", "always", "another",
	}
	techWords = []string{
		"server", "client", "database", "network", "protocol", "interface",
		"module", "function", "variable", "constant", "parameter", "return",
		"request", "response", "handler", "middleware", "router", "controller",
		"service", "repository", "factory", "builder", "adapter", "proxy",
	}
	nameWords = []string{
		"john", "jane", "michael", "sarah", "david", "emily", "robert", "lisa",
		"william", "jennifer", "james", "patricia", "charles", "elizabeth",
	}
	rareWords = []string{
		"xylophone", "quizzical", "zephyr", "fjord", "sphinx", "buzzing",
	}
)

func generateDocument(rng *rand.Rand, minWords, maxWords int) string {
	numWords := minWords + rng.Intn(maxWords-minWords+1)
	words := make([]string, numWords)

	for i := 0; i < numWords; i++ {
		switch rng.Intn(10) {
		case 0:
			words[i] = techWords[rng.Intn(len(techWords))]
		case 1:
			words[i] = nameWords[rng.Intn(len(nameWords))]
		case 2:
			if rng.Intn(100) < 5 {
				words[i] = rareWords[rng.Intn(len(rareWords))]
			} else {
				words[i] = commonWords[rng.Intn(len(commonWords))]
			}
		default:
			words[i] = commonWords[rng.Intn(len(commonWords))]
		}
	}

	return strings.Join(words, " ")
}
