package transforms

import (
	"fmt"
	"strings"
	"sync"
)

type Registry struct {
	transformerMap sync.Map
}

func NewReg() *Registry {
	return new(Registry)
}

var regLogPrefix = "Connector Registry"

func (r *Registry) Init(config map[string]interface{}) []Transformer {

	var transformers []Transformer

	txs := strings.Split(strings.Replace(config[`transforms`].(string), " ", "", -1), `,`)

	// HelloCast
	// transformers : cast1, cast2
	// cast1.p1 : 100
	// cast2.p1 : 300
	for _ , tName := range txs {
		//tName = strings.Replace(tName, " ", "", -1)
		transType := config[fmt.Sprintf(`transforms.%v.type`, tName)].(string)
		switchTrans := strings.Split(transType, "$")[0]
		switch switchTrans {
		case `Cast`:
			spec := config[fmt.Sprintf(`transforms.%v.spec`, tName)]
			props := make([]CastProps, 0)
			if strings.Contains(spec.(string), ",") {
				specs := strings.Split(spec.(string), ",")
				for _, s := range specs {
					attrType := strings.Split(s, ":")
					props = append(props, CastProps{attrType[0], attrType[1]})
				}

			} else {
				props = append(props, CastProps{"", spec.(string)})
			}

			transformers = append(transformers, Cast{transType, props})
		case `Drop`:
			beha := config[fmt.Sprintf(`transforms.%v.schema.behavior`, tName)]

			transformers = append(transformers, Drop{transType, beha.(string)})
		case `ExtractField`:
			field := config[fmt.Sprintf(`transforms.%v.field`, tName)].(string)

			transformers = append(transformers, ExtractField{transType, field})
		case `ExtractTopic`:
			field := config[fmt.Sprintf(`transforms.%v.field`, tName)].(string)
			missOrNull := config[fmt.Sprintf(`transforms.%v.skip.missing.or.null`, tName)].(bool)

			transformers = append(transformers, ExtractTopic{transType, field, missOrNull})
		case `Flatten`:
			delimiter := config[fmt.Sprintf(`transforms.%v.delimiter`, tName)].(string)

			transformers = append(transformers, Flatten{transType, delimiter})
		case `HoistField`:
			field := config[fmt.Sprintf(`transforms.%v.field`, tName)].(string)

			transformers = append(transformers, HoistField{transType, field})
		case `InsertField`:
			field := config[fmt.Sprintf(`transforms.%v.static.field`, tName)].(string)
			value := config[fmt.Sprintf(`transforms.%v.static.key`, tName)]

			transformers = append(transformers, InsertField{transType, field, value})
		case `MaskField`:
			f := config[fmt.Sprintf(`transforms.%v.fields`, tName)].(string)
			f = strings.Replace(f, " ", "", -1)
			fields := strings.Split(f, ",")

			transformers = append(transformers, MaskField{transType, fields})
		case `ReplaceField`:
			rp := make([]ReplaceFieldProps, 0)
			rn := config[fmt.Sprintf(`transforms.%v.renames`, tName)].(string)
			bl := config[fmt.Sprintf(`transforms.%v.blacklist`, tName)].(string)

			bl = strings.Replace(bl, " ", "", -1)
			rn = strings.Replace(rn, " ", "", -1)
			renames := strings.Split(rn, ",")
			for _, rename := range renames {
				props := strings.Split(rename, ":")
				rp = append(rp, ReplaceFieldProps{props[0], props[1]})
			}

			transformers = append(transformers, ReplaceField{transType, strings.Split(bl, ","), rp})
		case `ValueToKey`:
			f := config[fmt.Sprintf(`transforms.%v.fields`, tName)].(string)
			f = strings.Replace(f, " ", "", -1)
			fields := strings.Split(f, ",")

			transformers = append(transformers, ValueToKey{fields})
		}
	}

	return transformers
}

func (r *Registry) Get(name string) []Transformer {
	res, _ := r.transformerMap.Load(name)
	return res.([]Transformer)
}
