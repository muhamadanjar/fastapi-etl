"""
Email template engine implementation.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path
import json

try:
    from jinja2 import Environment, FileSystemLoader, BaseLoader, Template
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    logger.warning("Jinja2 not available. Install with: pip install jinja2")

from .base import TemplateEngineInterface, EmailTemplate
from ...core.exceptions import EmailError

logger = logging.getLogger(__name__)


class JinjaTemplateEngine(TemplateEngineInterface):
    """
    Jinja2-based template engine for emails.
    
    Features:
    - Jinja2 template rendering
    - Template inheritance
    - Custom filters and functions
    - Template caching
    - File-based template storage
    """
    
    def __init__(
        self,
        template_dir: Optional[str] = None,
        auto_reload: bool = False,
        cache_size: int = 400,
    ):
        """
        Initialize Jinja2 template engine.
        
        Args:
            template_dir: Directory containing templates
            auto_reload: Auto-reload templates on change
            cache_size: Template cache size
        """
        if not JINJA2_AVAILABLE:
            raise EmailError("Jinja2 not installed")
        
        self.template_dir = Path(template_dir) if template_dir else None
        self.auto_reload = auto_reload
        self.cache_size = cache_size
        
        # Initialize Jinja2 environment
        if self.template_dir and self.template_dir.exists():
            loader = FileSystemLoader(str(self.template_dir))
        else:
            loader = BaseLoader()
        
        self.env = Environment(
            loader=loader,
            auto_reload=auto_reload,
            cache_size=cache_size,
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Add custom filters
        self._add_custom_filters()
        
        # Template storage (for non-file templates)
        self._templates: Dict[str, EmailTemplate] = {}
        
        logger.info(f"Jinja2 template engine initialized (dir: {template_dir})")
    
    def _add_custom_filters(self) -> None:
        """Add custom Jinja2 filters for email templates."""
        
        def currency_filter(value: float, currency: str = "USD") -> str:
            """Format currency value."""
            if currency == "USD":
                return f"${value:,.2f}"
            elif currency == "EUR":
                return f"€{value:,.2f}"
            else:
                return f"{value:,.2f} {currency}"
        
        def date_filter(value, format: str = "%Y-%m-%d") -> str:
            """Format date value."""
            if hasattr(value, 'strftime'):
                return value.strftime(format)
            return str(value)
        
        def truncate_filter(value: str, length: int = 50, suffix: str = "...") -> str:
            """Truncate string to specified length."""
            if len(value) <= length:
                return value
            return value[:length - len(suffix)] + suffix
        
        # Register filters
        self.env.filters['currency'] = currency_filter
        self.env.filters['date'] = date_filter
        self.env.filters['truncate'] = truncate_filter
    
    async def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render template with context."""
        try:
            # Try to load from file system first
            if self.template_dir:
                try:
                    template = self.env.get_template(template_name)
                    return template.render(**context)
                except Exception:
                    pass
            
            # Try to load from memory
            if template_name in self._templates:
                email_template = self._templates[template_name]
                return await self.render_string(email_template.body_template, context)
            
            raise EmailError(f"Template not found: {template_name}")
            
        except Exception as e:
            logger.error(f"Template rendering failed for {template_name}: {e}")
            raise EmailError(f"Template rendering failed: {e}")
    
    async def render_string(self, template_string: str, context: Dict[str, Any]) -> str:
        """Render template string with context."""
        try:
            template = Template(template_string, environment=self.env)
            return template.render(**context)
        except Exception as e:
            logger.error(f"String template rendering failed: {e}")
            raise EmailError(f"String template rendering failed: {e}")
    
    async def load_template(self, template_name: str) -> EmailTemplate:
        """Load template by name."""
        if template_name in self._templates:
            return self._templates[template_name]
        
        # Try to load from file system
        if self.template_dir:
            template_file = self.template_dir / f"{template_name}.json"
            if template_file.exists():
                try:
                    with open(template_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    template = EmailTemplate(
                        name=template_name,
                        subject_template=data['subject'],
                        body_template=data['body'],
                        template_type=data.get('type', 'html'),
                        variables=data.get('variables', []),
                        metadata=data.get('metadata', {})
                    )
                    
                    # Cache in memory
                    self._templates[template_name] = template
                    return template
                    
                except Exception as e:
                    logger.error(f"Failed to load template {template_name}: {e}")
        
        raise EmailError(f"Template not found: {template_name}")
    
    async def save_template(self, template: EmailTemplate) -> bool:
        """Save template to storage."""
        try:
            # Save to memory
            self._templates[template.name] = template
            
            # Save to file system if directory is configured
            if self.template_dir:
                self.template_dir.mkdir(parents=True, exist_ok=True)
                template_file = self.template_dir / f"{template.name}.json"
                
                data = {
                    'subject': template.subject_template,
                    'body': template.body_template,
                    'type': template.template_type,
                    'variables': template.variables,
                    'metadata': template.metadata
                }
                
                with open(template_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Template saved: {template.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save template {template.name}: {e}")
            return False
    
    def add_global_variable(self, name: str, value: Any) -> None:
        """Add global variable available to all templates."""
        self.env.globals[name] = value
    
    def add_global_function(self, name: str, func: callable) -> None:
        """Add global function available to all templates."""
        self.env.globals[name] = func


class TemplateManager:
    """Template manager for email templates."""
    
    def __init__(self):
        self._engine: Optional[JinjaTemplateEngine] = None
    
    async def initialize(self, **config) -> None:
        """Initialize template engine."""
        from ...core.config import get_settings
        settings = get_settings()
        
        template_config = {
            'template_dir': config.get('template_dir', getattr(settings, 'EMAIL_TEMPLATE_DIR', 'templates/email')),
            'auto_reload': config.get('auto_reload', getattr(settings, 'EMAIL_TEMPLATE_AUTO_RELOAD', False)),
            'cache_size': config.get('cache_size', getattr(settings, 'EMAIL_TEMPLATE_CACHE_SIZE', 400)),
        }
        
        self._engine = JinjaTemplateEngine(**template_config)
        logger.info("Email template engine initialized")
    
    def get_engine(self) -> Optional[JinjaTemplateEngine]:
        """Get template engine instance."""
        return self._engine
    
    async def render_email_template(
        self, 
        template_name: str, 
        context: Dict[str, Any]
    ) -> Dict[str, str]:
        """Render email template returning subject and body."""
        if not self._engine:
            raise EmailError("Template engine not initialized")
        
        try:
            # Load template
            template = await self._engine.load_template(template_name)
            
            # Render subject and body
            subject = await self._engine.render_string(template.subject_template, context)
            body = await self._engine.render_string(template.body_template, context)
            
            return {
                'subject': subject,
                'body': body,
                'template_type': template.template_type
            }
            
        except Exception as e:
            logger.error(f"Failed to render email template {template_name}: {e}")
            raise EmailError(f"Email template rendering failed: {e}")


# Global template manager
template_manager = TemplateManager()