// split pattern like helloworld.message to topic helloworld and method message
export function splitPattern(pattern: string) {
	//check if pattern contains a dot, to split topic and subscriber
	if (pattern.indexOf('.') > -1) {
		const parts = pattern.split('.', 2);
		// check for empty parts
		if (!!parts[0] && !!parts[1]) {
			return {
				topic: parts[0],
				method: parts[1]
			};
		}
	}
	return {
		topic: pattern,
		method: undefined
	};
}

export function getSubscriptionName(svcName: string, method: string | undefined, direction?: 'request' | 'reply') {
	const name = ((method || '') + '_' + (svcName || '')).replace(/(^_|_$)/, '');
	if (direction === 'reply') {
		return name + '.reply';
	}
	return name;
}

export function arrayUnique<T>(array: T[]) {
	var a = array.concat();
	for (var i = 0; i < a.length; ++i) {
		for (var j = i + 1; j < a.length; ++j) {
			if (a[i] === a[j]) a.splice(j--, 1);
		}
	}
	return a;
}
