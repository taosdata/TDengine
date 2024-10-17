<template>
	<svg :style="{fill:color?color:''}">
		<use :xlink:href="'#icon-' + name"/>
	</svg>
</template>

<script>
export default {
	props: {
		name: String,
		color: {
			type: String,
			deafult: null
		}
	}
}
</script>
