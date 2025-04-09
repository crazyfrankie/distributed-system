package lab1

type option struct {
	mapFunc    MapFunc
	reduceFunc ReduceFunc
}

type Option func(*option)

func defaultOption() *option {
	return &option{
		mapFunc:    defaultMap,
		reduceFunc: defaultReduce,
	}
}

func WithMapFunc(f MapFunc) Option {
	return func(o *option) {
		o.mapFunc = f
	}
}

func WithReduceFunc(f ReduceFunc) Option {
	return func(o *option) {
		o.reduceFunc = f
	}
}
