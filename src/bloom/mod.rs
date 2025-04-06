use std::io;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// A simple Bloom filter implementation
pub struct BloomFilter {
    bits: Vec<bool>,
    num_hash_functions: usize,
    size: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with the given size and desired false positive rate
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size and number of hash functions
        let size = Self::optimal_size(expected_elements, false_positive_rate);
        let num_hash_functions = Self::optimal_hash_count(size, expected_elements);

        BloomFilter {
            bits: vec![false; size],
            num_hash_functions,
            size,
        }
    }

    /// Calculate optimal size based on expected elements and false positive rate
    fn optimal_size(expected_elements: usize, false_positive_rate: f64) -> usize {
        let size = -(expected_elements as f64 * false_positive_rate.ln()) / (2.0_f64.ln().powi(2));
        size.ceil() as usize
    }

    /// Calculate optimal number of hash functions
    fn optimal_hash_count(size: usize, expected_elements: usize) -> usize {
        let count = (size as f64 / expected_elements as f64) * 2.0_f64.ln();
        count.ceil() as usize
    }

    /// Insert an element into the Bloom filter
    pub fn insert<T: Hash + ?Sized>(&mut self, element: &T) {
        for i in 0..self.num_hash_functions {
            let position = self.hash_position(element, i);
            self.bits[position] = true;
        }
    }

    /// Check if an element might exist in the set
    pub fn might_contain<T: Hash + ?Sized>(&self, element: &T) -> bool {
        for i in 0..self.num_hash_functions {
            let position = self.hash_position(element, i);
            if !self.bits[position] {
                return false; // Definitely not in set
            }
        }
        true // Might be in set
    }

    /// Calculate hash position for an element with a seed
    fn hash_position<T: Hash + ?Sized>(&self, element: &T, seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        element.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize) % self.size
    }

    /// Serialize the Bloom filter to a byte vector
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        
        // Write size and hash function count
        bytes.extend_from_slice(&(self.size as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.num_hash_functions as u32).to_le_bytes());
        
        // Convert bits to bytes
        let mut current_byte = 0u8;
        let mut bit_count = 0;
        
        for &bit in &self.bits {
            if bit {
                current_byte |= 1 << bit_count;
            }
            
            bit_count += 1;
            if bit_count == 8 {
                bytes.push(current_byte);
                current_byte = 0;
                bit_count = 0;
            }
        }
        
        // Push the last byte if there are remaining bits
        if bit_count > 0 {
            bytes.push(current_byte);
        }
        
        bytes
    }

    /// Deserialize a Bloom filter from bytes
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Bloom filter data"));
        }
        
        // Read size and hash function count
        let size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let num_hash_functions = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        
        // Read bit array
        let mut bits = vec![false; size];
        let mut byte_index = 8; // Start after the header
        let mut bit_index = 0;
        
        while bit_index < size && byte_index < bytes.len() {
            let byte = bytes[byte_index];
            
            for i in 0..8 {
                if bit_index >= size {
                    break;
                }
                
                bits[bit_index] = (byte & (1 << i)) != 0;
                bit_index += 1;
            }
            
            byte_index += 1;
        }
        
        Ok(BloomFilter {
            bits,
            num_hash_functions,
            size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(100, 0.01);
        
        // Insert some elements
        filter.insert("apple");
        filter.insert("banana");
        filter.insert("cherry");
        
        // Check containment
        assert!(filter.might_contain("apple"));
        assert!(filter.might_contain("banana"));
        assert!(filter.might_contain("cherry"));
        
        // Check false negatives (should never happen)
        assert!(filter.might_contain("apple"));
        
        // Check something not in the set (might get false positive)
        let _not_present = filter.might_contain("dragonfruit");
        // Note: We can't assert !not_present because of false positives
    }
    
    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::new(100, 0.01);
        
        // Insert some elements
        filter.insert("apple");
        filter.insert("banana");
        filter.insert("cherry");
        
        // Serialize
        let bytes = filter.to_bytes();
        
        // Deserialize
        let restored_filter = BloomFilter::from_bytes(&bytes).unwrap();
        
        // Verify the restored filter works correctly
        assert!(restored_filter.might_contain("apple"));
        assert!(restored_filter.might_contain("banana"));
        assert!(restored_filter.might_contain("cherry"));
    }
}
