# ==============================================
# app/transformers/entity_matcher.py
# ==============================================
import re
from typing import Dict, List, Any, Optional, Tuple, Union, Set
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import json
import hashlib
from difflib import SequenceMatcher
import Levenshtein
from fuzzywuzzy import fuzz, process

from .base_transformer import BaseTransformer, TransformationResult, TransformationStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

class MatchingStrategy(Enum):
    """Entity matching strategies"""
    EXACT = "exact"
    FUZZY = "fuzzy"
    PHONETIC = "phonetic"
    SEMANTIC = "semantic"
    COMPOSITE = "composite"
    MACHINE_LEARNING = "ml"

class MatchingAlgorithm(Enum):
    """Matching algorithms"""
    LEVENSHTEIN = "levenshtein"
    JARO_WINKLER = "jaro_winkler"
    COSINE = "cosine"
    JACCARD = "jaccard"
    SOUNDEX = "soundex"
    METAPHONE = "metaphone"
    FUZZY_WUZZY = "fuzzy_wuzzy"

@dataclass
class MatchingRule:
    """Configuration for entity matching rule"""
    field_name: str
    strategy: MatchingStrategy
    algorithm: MatchingAlgorithm
    threshold: float
    weight: float = 1.0
    is_blocking: bool = False  # Use for blocking/indexing
    preprocessing: List[str] = None
    parameters: Dict[str, Any] = None

@dataclass
class EntityMatch:
    """Result of entity matching"""
    source_entity: Dict[str, Any]
    target_entity: Dict[str, Any]
    match_score: float
    field_scores: Dict[str, float]
    is_duplicate: bool
    confidence_level: str  # 'high', 'medium', 'low'
    match_reasons: List[str]

class EntityMatcher(BaseTransformer):
    """
    Entity matching transformer that handles:
    - Duplicate detection and deduplication
    - Entity resolution and linkage
    - Fuzzy matching algorithms
    - Phonetic matching
    - Semantic similarity
    - Composite scoring
    - Blocking strategies for performance
    - Machine learning-based matching
    - Person name matching
    - Address matching
    - Company name matching
    """
    
    def __init__(self, db_session, job_execution_id: Optional[str] = None, **kwargs):
        """
        Initialize entity matcher
        
        Args:
            db_session: Database session
            job_execution_id: Job execution ID for tracking
            **kwargs: Additional configuration
        """
        super().__init__(db_session, job_execution_id, **kwargs)
        
        # Matching configuration
        self.matching_rules = self._parse_matching_rules(kwargs.get('matching_rules', {}))
        self.global_threshold = kwargs.get('global_threshold', 0.8)
        self.enable_blocking = kwargs.get('enable_blocking', True)
        self.blocking_fields = kwargs.get('blocking_fields', [])
        self.max_comparisons = kwargs.get('max_comparisons', 10000)
        
        # Deduplication settings
        self.deduplicate_within_batch = kwargs.get('deduplicate_within_batch', True)
        self.deduplicate_against_existing = kwargs.get('deduplicate_against_existing', True)
        self.merge_strategy = kwargs.get('merge_strategy', 'best_quality')  # 'first', 'last', 'merge', 'best_quality'
        
        # Performance settings
        self.use_indexing = kwargs.get('use_indexing', True)
        self.index_fields = kwargs.get('index_fields', [])
        self.parallel_processing = kwargs.get('parallel_processing', False)
        
        # Entity storage for comparison
        self.entity_index = {}
        self.processed_entities = []
        self.duplicate_groups = []
        
        # Preprocessing functions
        self.preprocessing_functions = {
            'lowercase': lambda x: str(x).lower(),
            'uppercase': lambda x: str(x).upper(),
            'strip': lambda x: str(x).strip(),
            'remove_spaces': lambda x: re.sub(r'\s+', '', str(x)),
            'remove_punctuation': lambda x: re.sub(r'[^\w\s]', '', str(x)),
            'normalize_whitespace': lambda x: ' '.join(str(x).split()),
            'remove_titles': self._remove_titles,
            'normalize_company': self._normalize_company_name,
            'phonetic_normalize': self._phonetic_normalize,
        }
        
        # Phonetic algorithms
        self.phonetic_algorithms = {
            'soundex': self._soundex,
            'metaphone': self._metaphone,
            'double_metaphone': self._double_metaphone,
        }
        
        # Company name suffixes for normalization
        self.company_suffixes = [
            'ltd', 'limited', 'inc', 'incorporated', 'corp', 'corporation',
            'llc', 'pt', 'cv', 'tbk', 'persero', 'perum', 'bumd',
            'co', 'company', 'group', 'holding', 'international'
        ]
        
        # Person name titles for removal
        self.name_titles = [
            'mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'sir', 'madam',
            'bapak', 'ibu', 'pak', 'bu', 'drs', 'ir', 'st', 'se'
        ]
    
    def _parse_matching_rules(self, rules_config: Dict) -> List[MatchingRule]:
        """Parse matching rules from configuration"""
        rules = []
        
        for field_name, rule_config in rules_config.items():
            if not isinstance(rule_config, list):
                rule_config = [rule_config]
            
            for config in rule_config:
                rule = MatchingRule(
                    field_name=field_name,
                    strategy=MatchingStrategy(config.get('strategy', 'fuzzy')),
                    algorithm=MatchingAlgorithm(config.get('algorithm', 'levenshtein')),
                    threshold=config.get('threshold', 0.8),
                    weight=config.get('weight', 1.0),
                    is_blocking=config.get('is_blocking', False),
                    preprocessing=config.get('preprocessing', []),
                    parameters=config.get('parameters', {})
                )
                rules.append(rule)
        
        return rules
    
    async def validate_config(self) -> Tuple[bool, List[str]]:
        """Validate entity matcher configuration"""
        errors = []
        
        # Validate matching rules
        if not self.matching_rules:
            errors.append("No matching rules defined")
        
        # Validate thresholds
        for rule in self.matching_rules:
            if not 0.0 <= rule.threshold <= 1.0:
                errors.append(f"Invalid threshold {rule.threshold} for field '{rule.field_name}'")
            if not 0.0 <= rule.weight <= 10.0:
                errors.append(f"Invalid weight {rule.weight} for field '{rule.field_name}'")
        
        # Validate global threshold
        if not 0.0 <= self.global_threshold <= 1.0:
            errors.append(f"Invalid global threshold: {self.global_threshold}")
        
        # Validate merge strategy
        valid_strategies = ['first', 'last', 'merge', 'best_quality']
        if self.merge_strategy not in valid_strategies:
            errors.append(f"Invalid merge strategy: {self.merge_strategy}")
        
        return len(errors) == 0, errors
    
    async def transform_record(self, record: Dict[str, Any]) -> TransformationResult:
        """
        Match a single record against existing entities
        
        Args:
            record: Input record to match
            
        Returns:
            TransformationResult with matching results
        """
        try:
            matched_record = record.copy()
            warnings = []
            metadata = {
                'matching_performed': True,
                'matches_found': [],
                'is_duplicate': False,
                'duplicate_of': None,
                'match_score': 0.0,
                'blocking_used': self.enable_blocking,
                'comparisons_made': 0
            }
            
            # Preprocess record for matching
            preprocessed_record = await self._preprocess_record(record)
            
            # Find potential matches using blocking if enabled
            if self.enable_blocking:
                candidates = await self._find_blocking_candidates(preprocessed_record)
            else:
                candidates = self.processed_entities.copy()
            
            metadata['comparisons_made'] = len(candidates)
            
            # Perform matching against candidates
            matches = []
            for candidate in candidates:
                match_result = await self._compare_entities(preprocessed_record, candidate)
                if match_result.match_score >= self.global_threshold:
                    matches.append(match_result)
            
            # Sort matches by score (highest first)
            matches.sort(key=lambda x: x.match_score, reverse=True)
            
            if matches:
                best_match = matches[0]
                metadata['matches_found'] = [
                    {
                        'entity_id': self._generate_entity_id(match.target_entity),
                        'match_score': match.match_score,
                        'confidence': match.confidence_level,
                        'reasons': match.match_reasons
                    }
                    for match in matches[:5]  # Top 5 matches
                ]
                
                # Check if it's a duplicate
                if best_match.is_duplicate:
                    metadata['is_duplicate'] = True
                    metadata['duplicate_of'] = self._generate_entity_id(best_match.target_entity)
                    metadata['match_score'] = best_match.match_score
                    
                    # Apply merge strategy
                    if self.merge_strategy == 'merge':
                        matched_record = await self._merge_entities(record, best_match.target_entity)
                    elif self.merge_strategy == 'best_quality':
                        matched_record = await self._select_best_quality_entity(record, best_match.target_entity)
                    # For 'first' and 'last' strategies, we keep the original record
                    
                    warnings.append(f"Duplicate entity detected with {best_match.match_score:.2f} confidence")
                else:
                    # Similar but not duplicate
                    warnings.append(f"Similar entity found with {best_match.match_score:.2f} score")
            
            # Add to entity index for future comparisons
            if not metadata['is_duplicate'] or self.merge_strategy in ['merge', 'best_quality']:
                await self._add_to_index(matched_record)
            
            # Calculate quality score
            quality_score = self._calculate_matching_quality_score(matched_record, metadata)
            metadata['quality_score'] = quality_score
            
            # Add entity metadata
            matched_record['_entity_id'] = self._generate_entity_id(matched_record)
            matched_record['_match_metadata'] = {
                'is_duplicate': metadata['is_duplicate'],
                'match_score': metadata['match_score'],
                'processed_at': datetime.utcnow().isoformat()
            }
            
            return TransformationResult(
                status=TransformationStatus.SUCCESS,
                data=matched_record,
                warnings=warnings,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"Error matching entity: {str(e)}")
            return TransformationResult(
                status=TransformationStatus.FAILED,
                errors=[f"Entity matching failed: {str(e)}"]
            )
    
    async def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess record for matching"""
        preprocessed = {}
        
        for field_name, value in record.items():
            if value is None:
                preprocessed[field_name] = None
                continue
            
            # Find matching rules for this field
            field_rules = [rule for rule in self.matching_rules if rule.field_name == field_name]
            
            processed_value = str(value)
            
            # Apply preprocessing for each rule
            for rule in field_rules:
                if rule.preprocessing:
                    for preprocess_func in rule.preprocessing:
                        if preprocess_func in self.preprocessing_functions:
                            processed_value = self.preprocessing_functions[preprocess_func](processed_value)
            
            preprocessed[field_name] = processed_value
            
            # Keep original value for reference
            preprocessed[f"_original_{field_name}"] = value
        
        return preprocessed
    
    async def _find_blocking_candidates(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find candidate entities using blocking strategy"""
        candidates = []
        
        if not self.blocking_fields:
            # Use all entities if no blocking fields specified
            return self.processed_entities.copy()
        
        # Generate blocking keys
        blocking_keys = []
        for field in self.blocking_fields:
            value = record.get(field)
            if value:
                # Create multiple blocking keys for fuzzy blocking
                blocking_keys.extend(self._generate_blocking_keys(str(value)))
        
        # Find entities with matching blocking keys
        seen_entities = set()
        for entity in self.processed_entities:
            entity_id = self._generate_entity_id(entity)
            if entity_id in seen_entities:
                continue
            
            for field in self.blocking_fields:
                entity_value = entity.get(field)
                if entity_value:
                    entity_keys = self._generate_blocking_keys(str(entity_value))
                    if any(key in blocking_keys for key in entity_keys):
                        candidates.append(entity)
                        seen_entities.add(entity_id)
                        break
        
        return candidates
    
    def _generate_blocking_keys(self, value: str) -> List[str]:
        """Generate blocking keys for a value"""
        keys = []
        
        # Original value
        keys.append(value.lower().strip())
        
        # First few characters
        for length in [2, 3, 4]:
            if len(value) >= length:
                keys.append(value[:length].lower())
        
        # Soundex
        keys.append(self._soundex(value))
        
        # Remove spaces and punctuation
        cleaned = re.sub(r'[^\w]', '', value).lower()
        if cleaned != value.lower():
            keys.append(cleaned)
        
        # First and last name for person names
        words = value.split()
        if len(words) >= 2:
            keys.append(f"{words[0]}_{words[-1]}".lower())
        
        return list(set(keys))  # Remove duplicates
    
    async def _compare_entities(self, entity1: Dict[str, Any], entity2: Dict[str, Any]) -> EntityMatch:
        """Compare two entities using configured matching rules"""
        field_scores = {}
        total_weighted_score = 0.0
        total_weights = 0.0
        match_reasons = []
        
        for rule in self.matching_rules:
            field_name = rule.field_name
            value1 = entity1.get(field_name)
            value2 = entity2.get(field_name)
            
            # Skip if either value is missing
            if value1 is None or value2 is None:
                continue
            
            # Calculate field similarity
            field_score = await self._calculate_field_similarity(value1, value2, rule)
            field_scores[field_name] = field_score
            
            # Add to weighted total
            weighted_score = field_score * rule.weight
            total_weighted_score += weighted_score
            total_weights += rule.weight
            
            # Add match reason if score is high
            if field_score >= rule.threshold:
                match_reasons.append(f"{field_name}: {field_score:.2f}")
        
        # Calculate overall match score
        if total_weights > 0:
            match_score = total_weighted_score / total_weights
        else:
            match_score = 0.0
        
        # Determine if it's a duplicate
        is_duplicate = match_score >= self.global_threshold
        
        # Determine confidence level
        if match_score >= 0.9:
            confidence = 'high'
        elif match_score >= 0.7:
            confidence = 'medium'
        else:
            confidence = 'low'
        
        return EntityMatch(
            source_entity=entity1,
            target_entity=entity2,
            match_score=match_score,
            field_scores=field_scores,
            is_duplicate=is_duplicate,
            confidence_level=confidence,
            match_reasons=match_reasons
        )
    
    async def _calculate_field_similarity(self, value1: str, value2: str, rule: MatchingRule) -> float:
        """Calculate similarity between two field values"""
        if not value1 or not value2:
            return 0.0
        
        value1_str = str(value1)
        value2_str = str(value2)
        
        if rule.strategy == MatchingStrategy.EXACT:
            return 1.0 if value1_str == value2_str else 0.0
        
        elif rule.strategy == MatchingStrategy.FUZZY:
            return await self._calculate_fuzzy_similarity(value1_str, value2_str, rule.algorithm)
        
        elif rule.strategy == MatchingStrategy.PHONETIC:
            return await self._calculate_phonetic_similarity(value1_str, value2_str, rule.algorithm)
        
        elif rule.strategy == MatchingStrategy.SEMANTIC:
            return await self._calculate_semantic_similarity(value1_str, value2_str, rule.parameters)
        
        elif rule.strategy == MatchingStrategy.COMPOSITE:
            return await self._calculate_composite_similarity(value1_str, value2_str, rule.parameters)
        
        else:
            # Default to fuzzy matching
            return await self._calculate_fuzzy_similarity(value1_str, value2_str, MatchingAlgorithm.LEVENSHTEIN)
    
    async def _calculate_fuzzy_similarity(self, value1: str, value2: str, algorithm: MatchingAlgorithm) -> float:
        """Calculate fuzzy similarity using specified algorithm"""
        if algorithm == MatchingAlgorithm.LEVENSHTEIN:
            distance = Levenshtein.distance(value1, value2)
            max_len = max(len(value1), len(value2))
            return 1.0 - (distance / max_len) if max_len > 0 else 1.0
        
        elif algorithm == MatchingAlgorithm.JARO_WINKLER:
            return Levenshtein.jaro_winkler(value1, value2)
        
        elif algorithm == MatchingAlgorithm.FUZZY_WUZZY:
            return fuzz.ratio(value1, value2) / 100.0
        
        elif algorithm == MatchingAlgorithm.COSINE:
            return self._cosine_similarity(value1, value2)
        
        elif algorithm == MatchingAlgorithm.JACCARD:
            return self._jaccard_similarity(value1, value2)
        
        else:
            # Default to Levenshtein
            return await self._calculate_fuzzy_similarity(value1, value2, MatchingAlgorithm.LEVENSHTEIN)
    
    async def _calculate_phonetic_similarity(self, value1: str, value2: str, algorithm: MatchingAlgorithm) -> float:
        """Calculate phonetic similarity"""
        if algorithm == MatchingAlgorithm.SOUNDEX:
            soundex1 = self._soundex(value1)
            soundex2 = self._soundex(value2)
            return 1.0 if soundex1 == soundex2 else 0.0
        
        elif algorithm == MatchingAlgorithm.METAPHONE:
            metaphone1 = self._metaphone(value1)
            metaphone2 = self._metaphone(value2)
            return 1.0 if metaphone1 == metaphone2 else 0.0
        
        else:
            # Default to Soundex
            return await self._calculate_phonetic_similarity(value1, value2, MatchingAlgorithm.SOUNDEX)
    
    async def _calculate_semantic_similarity(self, value1: str, value2: str, parameters: Dict[str, Any]) -> float:
        """Calculate semantic similarity (placeholder for advanced NLP)"""
        # This is a placeholder for semantic similarity
        # In production, you might use word embeddings, BERT, or other NLP models
        
        # Simple word overlap similarity for now
        words1 = set(value1.lower().split())
        words2 = set(value2.lower().split())
        
        if not words1 and not words2:
            return 1.0
        if not words1 or not words2:
            return 0.0
        
        intersection = words1 & words2
        union = words1 | words2
        
        return len(intersection) / len(union)
    
    async def _calculate_composite_similarity(self, value1: str, value2: str, parameters: Dict[str, Any]) -> float:
        """Calculate composite similarity using multiple algorithms"""
        algorithms = parameters.get('algorithms', ['levenshtein', 'jaro_winkler'])
        weights = parameters.get('weights', [1.0] * len(algorithms))
        
        total_score = 0.0
        total_weight = 0.0
        
        for i, algorithm_name in enumerate(algorithms):
            try:
                algorithm = MatchingAlgorithm(algorithm_name)
                score = await self._calculate_fuzzy_similarity(value1, value2, algorithm)
                weight = weights[i] if i < len(weights) else 1.0
                
                total_score += score * weight
                total_weight += weight
            except ValueError:
                continue
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def _cosine_similarity(self, value1: str, value2: str) -> float:
        """Calculate cosine similarity between two strings"""
        # Convert to character n-grams
        ngrams1 = self._get_character_ngrams(value1, 2)
        ngrams2 = self._get_character_ngrams(value2, 2)
        
        # Calculate cosine similarity
        intersection = ngrams1 & ngrams2
        if not ngrams1 or not ngrams2:
            return 0.0
        
        numerator = len(intersection)
        denominator = (len(ngrams1) * len(ngrams2)) ** 0.5
        
        return numerator / denominator if denominator > 0 else 0.0
    
    def _jaccard_similarity(self, value1: str, value2: str) -> float:
        """Calculate Jaccard similarity between two strings"""
        ngrams1 = self._get_character_ngrams(value1, 2)
        ngrams2 = self._get_character_ngrams(value2, 2)
        
        intersection = ngrams1 & ngrams2
        union = ngrams1 | ngrams2
        
        return len(intersection) / len(union) if union else 1.0
    
    def _get_character_ngrams(self, text: str, n: int) -> Set[str]:
        """Get character n-grams from text"""
        text = text.lower()
        return set(text[i:i+n] for i in range(len(text) - n + 1))
    
    # Phonetic algorithms
    def _soundex(self, name: str) -> str:
        """Generate Soundex code for name"""
        name = name.upper()
        soundex = ""
        
        # Keep the first letter
        soundex += name[0]
        
        # Define the digit mappings
        mapping = {
            'BFPV': '1',
            'CGJKQSXZ': '2',
            'DT': '3',
            'L': '4',
            'MN': '5',
            'R': '6'
        }
        
        for char in name[1:]:
            for chars, digit in mapping.items():
                if char in chars:
                    soundex += digit
                    break
        
        # Remove duplicates and vowels, pad with zeros
        soundex = soundex[0] + ''.join([soundex[i] for i in range(1, len(soundex)) if soundex[i] != soundex[i-1]])
        soundex = soundex.replace('A', '').replace('E', '').replace('I', '').replace('O', '').replace('U', '').replace('H', '').replace('W', '').replace('Y', '')
        soundex = (soundex + '0000')[:4]
        
        return soundex
    
    def _metaphone(self, name: str) -> str:
        """Generate Metaphone code for name (simplified version)"""
        # This is a simplified Metaphone implementation
        # For production use, consider using a proper library
        name = name.upper()
        
        # Basic transformations
        replacements = [
            ('PH', 'F'), ('GH', 'F'), ('CK', 'K'), ('SCH', 'SK'),
            ('TH', 'T'), ('SH', 'S'), ('CH', 'K')
        ]
        
        for old, new in replacements:
            name = name.replace(old, new)
        
        # Remove vowels except at the beginning
        if name:
            result = name[0]
            for char in name[1:]:
                if char not in 'AEIOU':
                    result += char
        else:
            result = name
        
        return result[:4]  # Limit to 4 characters
    
    def _double_metaphone(self, name: str) -> Tuple[str, str]:
        """Generate Double Metaphone codes (simplified)"""
        # This is a placeholder for Double Metaphone
        # For production, use a proper library like fuzzy
        metaphone = self._metaphone(name)
        return metaphone, metaphone
    
    # Preprocessing functions
    def _remove_titles(self, name: str) -> str:
        """Remove common titles from names"""
        words = name.lower().split()
        filtered_words = [word for word in words if word not in self.name_titles]
        return ' '.join(filtered_words)
    
    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name"""
        name = name.lower().strip()
        
        # Remove common suffixes
        for suffix in self.company_suffixes:
            pattern = rf'\b{suffix}\.?\b'
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        return name
    
    def _phonetic_normalize(self, name: str) -> str:
        """Normalize name for phonetic matching"""
        # Remove punctuation and extra spaces
        name = re.sub(r'[^\w\s]', '', name)
        name = ' '.join(name.split())
        return name.lower()
    
    async def _merge_entities(self, entity1: Dict[str, Any], entity2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two entities into one"""
        merged = {}
        
        # Get all fields from both entities
        all_fields = set(entity1.keys()) | set(entity2.keys())
        
        for field in all_fields:
            value1 = entity1.get(field)
            value2 = entity2.get(field)
            
            # Skip internal fields
            if field.startswith('_'):
                continue
            
            # If both have values, prefer the more complete one
            if value1 and value2:
                if len(str(value1)) >= len(str(value2)):
                    merged[field] = value1
                else:
                    merged[field] = value2
            elif value1:
                merged[field] = value1
            elif value2:
                merged[field] = value2
        
        return merged
    
    async def _select_best_quality_entity(self, entity1: Dict[str, Any], entity2: Dict[str, Any]) -> Dict[str, Any]:
        """Select the entity with better data quality"""
        # Calculate quality scores
        score1 = self._calculate_entity_quality_score(entity1)
        score2 = self._calculate_entity_quality_score(entity2)
        
        return entity1 if score1 >= score2 else entity2
    
    def _calculate_entity_quality_score(self, entity: Dict[str, Any]) -> float:
        """Calculate quality score for an entity"""
        total_fields = 0
        filled_fields = 0
        
        for field, value in entity.items():
            if field.startswith('_'):
                continue
            
            total_fields += 1
            if value and str(value).strip():
                filled_fields += 1
        
        completeness = filled_fields / total_fields if total_fields > 0 else 0.0
        
        # Additional quality factors can be added here
        # For example: data format consistency, length of values, etc.
        
        return completeness
    
    async def _add_to_index(self, entity: Dict[str, Any]):
        """Add entity to index for future matching"""
        entity_id = self._generate_entity_id(entity)
        
        # Add to main storage
        self.processed_entities.append(entity)
        
        # Add to index for blocking
        if self.use_indexing:
            for field in self.index_fields:
                value = entity.get(field)
                if value:
                    index_key = str(value).lower()
                    if index_key not in self.entity_index:
                        self.entity_index[index_key] = []
                    self.entity_index[index_key].append(entity_id)
    
    def _generate_entity_id(self, entity: Dict[str, Any]) -> str:
        """Generate unique ID for entity"""
        # Create hash from key fields
        key_fields = []
        for rule in self.matching_rules:
            if rule.is_blocking:
                value = entity.get(rule.field_name)
                if value:
                    key_fields.append(f"{rule.field_name}:{value}")
        
        if not key_fields:
            # Fallback to all fields
            key_fields = [f"{k}:{v}" for k, v in entity.items() if not str(k).startswith('_')]
        
        key_string = '|'.join(sorted(key_fields))
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _calculate_matching_quality_score(self, entity: Dict[str, Any], metadata: Dict[str, Any]) -> float:
        """Calculate quality score for matching process"""
        base_score = self._calculate_entity_quality_score(entity)
        
        # Adjust score based on matching results
        matching_score = 1.0
        
        # Penalty for duplicates
        if metadata.get('is_duplicate', False):
            match_score = metadata.get('match_score', 0.0)
            if match_score > 0.95:
                # High confidence duplicate - significant penalty
                matching_score = 0.3
            elif match_score > 0.8:
                # Medium confidence duplicate - moderate penalty
                matching_score = 0.6
            else:
                # Low confidence duplicate - light penalty
                matching_score = 0.8
        
        # Bonus for unique entities
        elif metadata.get('matches_found'):
            # Found similar but not duplicate - slight bonus for uniqueness verification
            matching_score = 1.1
        
        # Bonus for successful blocking
        if metadata.get('blocking_used', False):
            comparisons = metadata.get('comparisons_made', 0)
            if comparisons < 100:  # Efficient blocking
                matching_score += 0.05
        
        # Combine scores
        final_score = (base_score * 0.7) + (min(matching_score, 1.0) * 0.3)
        
        return max(0.0, min(1.0, final_score))
    
    async def get_duplicate_groups(self) -> List[List[Dict[str, Any]]]:
        """Get groups of duplicate entities"""
        return self.duplicate_groups.copy()
    
    async def get_matching_statistics(self) -> Dict[str, Any]:
        """Get matching statistics"""
        total_entities = len(self.processed_entities)
        duplicate_count = sum(len(group) for group in self.duplicate_groups)
        unique_count = total_entities - duplicate_count
        
        return {
            'total_entities_processed': total_entities,
            'unique_entities': unique_count,
            'duplicate_entities': duplicate_count,
            'duplicate_groups': len(self.duplicate_groups),
            'deduplication_rate': (duplicate_count / total_entities * 100) if total_entities > 0 else 0.0,
            'matching_rules_count': len(self.matching_rules),
            'blocking_enabled': self.enable_blocking,
            'index_size': len(self.entity_index)
        }
    
    async def export_matching_results(self) -> Dict[str, Any]:
        """Export matching results for analysis"""
        return {
            'processed_entities': self.processed_entities,
            'duplicate_groups': self.duplicate_groups,
            'entity_index': dict(self.entity_index),
            'statistics': await self.get_matching_statistics(),
            'configuration': {
                'matching_rules': [
                    {
                        'field_name': rule.field_name,
                        'strategy': rule.strategy.value,
                        'algorithm': rule.algorithm.value,
                        'threshold': rule.threshold,
                        'weight': rule.weight
                    }
                    for rule in self.matching_rules
                ],
                'global_threshold': self.global_threshold,
                'merge_strategy': self.merge_strategy,
                'blocking_enabled': self.enable_blocking
            }
        }
    
    async def cleanup_transformation(self):
        """Cleanup resources after transformation"""
        try:
            # Clear large data structures
            self.entity_index.clear()
            self.processed_entities.clear()
            self.duplicate_groups.clear()
            self.uniqueness_cache.clear()
            
            self.logger.info("Entity matcher cleanup completed")
            
        except Exception as e:
            self.logger.warning(f"Error during entity matcher cleanup: {str(e)}")
        
        # Call parent cleanup
        await super().cleanup_transformation()
    
    def __str__(self) -> str:
        stats = f"processed={len(self.processed_entities)}, duplicates={sum(len(g) for g in self.duplicate_groups)}"
        return f"{self.__class__.__name__}({stats})"
    