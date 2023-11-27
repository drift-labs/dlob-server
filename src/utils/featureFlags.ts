// TODO : Is it worth adding proper infrastructure for feature flags? .. Would allow more powerful things like toggling them at runtime rather than being hardcoded
export const FEATURE_FLAGS = {
	OLD_ORACLE_PRICE_IN_L2: true, // TODO : Remove this once we're confident that NEW_ORACLE_DATA_IN_L2 works .. delete corresponding code
	NEW_ORACLE_DATA_IN_L2: true,
};

export default FEATURE_FLAGS;
